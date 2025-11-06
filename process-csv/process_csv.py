import argparse
import base64
import csv
import io
import json
import logging
from logging.config import dictConfig
import os.path
import re
import sys
import time
from typing import Optional

import requests
from tqdm import tqdm

template_re = re.compile(r"\[(.*?)\]|(\w[\w-]*)(?:\s+(as\s+%))?")
ann_type_re = re.compile(r"^(\S+)(?:\s+(.*))?$")
lt_amp_re = re.compile(r"&(amp|lt);")

logger = logging.getLogger(__name__)

accept_json = {"Accept": "application/json"}


def lt_amp_replacement(m):
    if m.group(1) == "amp":
        return "&"
    else:
        return "<"


def unescape_lt_amp(s: str):
    return lt_amp_re.sub(lt_amp_replacement, s)


def text_under(ann_type: str, separator: str):
    def resp_func(response):
        return separator.join(
            unescape_lt_amp(response["text"][slice(*(it["indices"]))]) for it in response["entities"].get(ann_type, [])
        )

    return resp_func


def response_to_column(ann_type: str, template: str, separator: str):
    def resp_func(response):
        chunks = []
        for ann in response["entities"].get(ann_type, []):

            def replacement(m):
                literal, feature, modifier = m.groups()
                if literal:
                    return literal
                elif feature == "text":
                    return unescape_lt_amp(response["text"][slice(*(ann["indices"]))])
                elif feature in ann:
                    if modifier == "as %":
                        return "{:.2%}".format(ann[feature])
                    else:
                        return str(ann[feature])
                else:
                    return f"{feature} not found"

            chunks.append(template_re.sub(replacement, template))

        return separator.join(chunks)

    return resp_func


def output_function(col: str, type_to_sel: dict):
    """
    Construct the function that will generate the output for a given column definition.

    Arguments:
        col: column definition, typically either "AnnotationType" representing the text under that
             annotation, or "AnnotationType feature" representing the value of the given feature of that
             annotation type
        type_to_sel: mapping of annotation types to their selectors from the service metadata

    Returns:
        tuple of the annotation selector that must be passed to the service in order to retrieve the
        type of annotation that the function will operate on, and the actual function that takes a GATE Cloud
        API response and extracts the appropriate value(s) from it
    """
    logger.info("Processing column definition: %s", col)
    ann_type, rest = ann_type_re.match(col.strip()).groups()
    if ":" in ann_type:
        ann_selector = ann_type
        ann_type = ann_type[ann_type.index(":") + 1 :]
    elif ann_type in type_to_sel:
        ann_selector = type_to_sel[ann_type]
    else:
        ann_selector = f":{ann_type}"

    if not rest or rest == "text":
        logger.info("Using text under annotation type %s", ann_type)
        fn = text_under(ann_type, ";")
    elif rest == "present?":
        logger.info("Existence check - 1 if annotation type %s is present, 0 otherwise", ann_type)
        fn = lambda resp: str(1 if len(resp["entities"].get(ann_type, [])) > 0 else 0)
    elif rest == "#count":
        logger.info("Annotation count - number of occurrences of annotation type %s", ann_type)
        fn = lambda resp: str(len(resp["entities"].get(ann_type, [])))
    else:
        # The full version
        logger.info("Annotation feature template")
        fn = response_to_column(ann_type, rest, ";")

    return ann_selector, fn


class CsvProcessor:
    def __init__(self, args, credentials):
        self.prev_rate_limit_remaining = -1
        self.request_start_time: Optional[float] = None

        self.separator = args.separator
        self.has_headers = args.has_headers
        logger.info("Columns separated by %s", json.dumps(self.separator))
        if self.has_headers:
            logger.info("Treating first row as column headers")
        else:
            logger.info("No column headers - columns referenced by index")

        self.endpoint = args.endpoint
        logger.info("Using GATE Cloud endpoint: %s", self.endpoint)

        self.session = requests.Session()
        self.session.headers["Accept"] = "application/json"
        if credentials:
            auth_header = "Basic " + base64.b64encode(bytes(credentials, "utf-8")).decode("ascii")
            self.session.headers["Authorization"] = auth_header

        # Parse the service metadata to get the appropriate annotation set name
        # for each annotation type
        self.type_to_sel = self.get_annotations_from_metadata()

        self.session.headers["Content-Type"] = args.mime_type

        logger.info("Text to be processed is column %s", args.text_column)
        self.text_column = args.text_column
        copy_columns = [c.strip() for c in args.copy_columns]
        self.copy_columns = [c for c in copy_columns if c]
        logger.info("Copying following columns to output: %s", self.copy_columns)
        output_columns = [c.strip() for c in args.results]
        self.output_columns = [c for c in output_columns if c]
        logger.info("Annotation output columns: %s", self.output_columns)
        selectors_and_functions = zip(*(output_function(c, self.type_to_sel) for c in self.output_columns))

        # output_function returns a (str, func) tuple so zip unpacks this into an iterable
        # of strings (selectors) and an iterable of functions.  De-duplicate selectors
        # and use them as the ?annotations=... query param, and save the functions in a
        # list for later use to unpack the responses
        self.ann_selectors = dict(annotations=frozenset(next(selectors_and_functions)))
        self.output_functions = list(next(selectors_and_functions))
        logger.info("Annotation selectors to send to service: %s", self.ann_selectors)

    def get_annotations_from_metadata(self):
        logger.info("Fetching service metadata")
        type_to_sel: dict[str, str] = {}
        with self.session.get(self.endpoint + "/metadata") as resp:
            if resp.status_code == 200:
                service_metadata = resp.json()
                all_selectors = []
                if service_metadata.get("defaultAnnotations"):
                    all_selectors.extend(sel.strip() for sel in service_metadata["defaultAnnotations"].split(","))
                if service_metadata.get("additionalAnnotations"):
                    all_selectors.extend(sel.strip() for sel in service_metadata["additionalAnnotations"].split(","))
                for selector in all_selectors:
                    as_name, _, ann_type = selector.partition(":")
                    if ann_type not in type_to_sel:
                        type_to_sel[ann_type] = selector
            else:
                logger.warning("Could not access service metadata")

        return type_to_sel

    def handle_rate_limit(self, response: requests.Response) -> float:
        # Logic:
        #
        # - if we've already hit the rate limit or quota then just wait until the retry time
        # - otherwise take the max of this request cost and the difference between remaining rate limit
        #   after this request and the remaining rate limit after the previous call (which might be more than
        #   one call used up if there's another run going in parallel)
        # - divide the time until rate limit reset by this number to get the wait time between calls that should
        #   "use up" that rate limit precisely by the reset time, and multiply by 1.05 so we don't actually hit
        #   the limit
        # - actual wait time before starting the next call is then this time minus "now" plus the time that
        #   this request _started_ (so we're limiting the start-to-start times rather than the end-to-start)
        # - if the final result is less than 0, return 0 (i.e. no need to wait at all)
        try:
            if (response.status_code == 429 or response.status_code == 402) and ("retry-after" in response.headers):
                # already hit the rate limit
                logger.info("Rate limit reached - waiting %s seconds", response.headers["retry-after"])
                return float(response.headers["retry-after"])

            try:
                this_rate_limit_remaining = int(response.headers["x-gate-rate-limit-calls"])
                time_until_reset = int(response.headers["x-gate-rate-limit-reset"])
            except ValueError:
                return 0.0

            used_limit_since_last_call = 1
            if 0 < self.prev_rate_limit_remaining < this_rate_limit_remaining:
                used_limit_since_last_call = this_rate_limit_remaining - self.prev_rate_limit_remaining
            self.prev_rate_limit_remaining = this_rate_limit_remaining

            wait_time_until_next_call = (
                (time_until_reset / this_rate_limit_remaining) * used_limit_since_last_call * 1.05
            )
            if self.request_start_time:
                wait_time_until_next_call += self.request_start_time - time.perf_counter()

            if wait_time_until_next_call > 0:
                return wait_time_until_next_call
            else:
                return 0.0
        finally:
            response.close()

    def run(self, in_file, in_encoding, out_file, out_encoding):
        # Record size of the input file for progress reporting
        in_file_size = os.path.getsize(in_file)
        logger.info("Reading input file '%s' of size %d bytes, with encoding %s", in_file, in_file_size, in_encoding)
        with open(in_file, "rb", buffering=8192) as in_binary:
            with io.TextIOWrapper(in_binary, encoding=in_encoding, newline="") as in_f:
                r = csv.reader(in_f, delimiter=self.separator)
                if self.has_headers:
                    first_row = next(r)
                    text_column = first_row.index(self.text_column)
                    copy_columns = [first_row.index(c) for c in self.copy_columns]
                else:
                    text_column = int(self.text_column) - 1
                    copy_columns = [(int(c) - 1) for c in self.copy_columns]

                logger.info("Writing to output file '%s' with encoding %s", out_file, out_encoding)
                with open(out_file, "w", encoding=out_encoding, newline="") as out_f:
                    w = csv.writer(out_f)
                    col_headers = []
                    if self.has_headers:
                        col_headers.extend(first_row[c] for c in copy_columns)
                    else:
                        col_headers.extend(f"Column {c + 1}" for c in copy_columns)
                    # status column for success or error message
                    col_headers.append("status")
                    # output columns from annotations
                    col_headers.extend(self.output_columns)
                    w.writerow(col_headers)

                    rate_limit_failures = 0
                    wait_before_next_call = 0.0

                    with tqdm(total=in_file_size, unit="B", unit_scale=True) as pbar:
                        in_file_pos = 0
                        for idx, row in enumerate(r):
                            text = row[text_column]
                            err_json = None
                            while True:
                                results = [row[c] for c in copy_columns]
                                # pass text to cloud service
                                try:
                                    if wait_before_next_call > 5.0:
                                        logger.info(
                                            "Waiting %.2f seconds before next API call for rate limiting",
                                            wait_before_next_call,
                                        )
                                    time.sleep(wait_before_next_call)
                                    self.request_start_time = time.perf_counter()
                                    response = self.session.post(self.endpoint, params=self.ann_selectors, data=text)
                                    if response.status_code == 200:
                                        resp_json = response.json()
                                        if "text" in resp_json and "entities" in resp_json:
                                            results.append("Success")
                                            results.extend(f(resp_json) for f in self.output_functions)
                                            break
                                    elif response.status_code == 429 or response.status_code == 402:
                                        # Rate limit or quota has been hit
                                        rate_limit_failures += 1
                                        if rate_limit_failures > 5:
                                            # something is very wrong, give up
                                            logger.error("Rate limit reached too many times")
                                            sys.exit(1)
                                        wait_before_next_call = self.handle_rate_limit(response)
                                    else:
                                        # Genuine error response
                                        try:
                                            err_json = response.json()
                                        except requests.JSONDecodeError:
                                            err_json = dict(message=response.text)
                                        break
                                except requests.RequestException as e:
                                    logger.exception("Error making API request")
                                    err_json = dict(message=str(e))
                                    break

                            wait_before_next_call = self.handle_rate_limit(response)

                            if err_json:
                                if "message" in err_json:
                                    results.append(f"Error: {err_json['message']}")
                                else:
                                    results.append(f"Error: {json.dumps(err_json)}")

                            w.writerow(results)

                            # update the progress bar
                            new_file_pos = in_binary.tell()
                            bytes_read = new_file_pos - in_file_pos
                            in_file_pos = new_file_pos
                            pbar.update(bytes_read)


def main():
    api_key = None
    api_password = None

    LOGGING_CONFIG = dict(
        version=1,
        disable_existing_loggers=False,
        formatters=dict(
            f=dict(
                format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
            ),
        ),
        handlers=dict(
            h={"class": "logging.StreamHandler", "formatter": "f", "level": logging.DEBUG},
        ),
        root=dict(
            handlers=["h"],
            level=logging.INFO,
        ),
    )

    try:
        import settings

        api_key = settings.API_KEY
        api_password = settings.API_PASSWORD
        LOGGING_CONFIG = settings.LOGGING_CONFIG
    except:
        # Ignore settings file
        pass

    dictConfig(LOGGING_CONFIG)

    parser = argparse.ArgumentParser(
        epilog="Process texts taken from a column in a CSV file with a GATE Cloud "
        "text annotation service.  The input of the tool should be a CSV/TSV "
        "file, and the output is another CSV file with one row per input row. "
        "The output row is constructed by optionally copying some of the columns "
        "from the input row, and then adding additional columns derived from the "
        "annotations created by the GATE Cloud service."
    )

    parser.add_argument(
        "--endpoint",
        required=True,
        help="GATE Cloud endpoint to call, typically copied from the 'Use this " "pipeline' section on GATE Cloud.",
    )

    if not api_key or not api_password:
        creds_group = parser.add_argument_group(
            "API credentials",
            "Credentials for the GATE Cloud API.  To set these permanently, "
            "create a file named settings.py in the current directory, with lines "
            "API_KEY = '...' and API_PASSWORD = '...' containing your GATE Cloud API "
            "key ID and password respectively.  If credentials are not provided you will be "
            "subject to the (very low) unauthenticated rate limits of cloud.gate.ac.uk.",
        )
    else:
        creds_group = parser.add_argument_group(
            "API credentials", "Override your stored credentials for the GATE Cloud API."
        )

    creds_group.add_argument("--api-key", help="API key ID from cloud.gate.ac.uk")
    creds_group.add_argument("--api-password", help="API key password from cloud.gate.ac.uk")

    input_group = parser.add_argument_group("Input settings")
    input_group.add_argument("--in", dest="in_file", required=True, help="The input CSV or TSV file to process")
    input_group.add_argument(
        "--html",
        dest="mime_type",
        action="store_const",
        default="text/plain",
        const="text/html",
        help="The 'text' in your CSV file is actually HTML",
    )
    input_group.add_argument(
        "--encoding",
        default="utf-8-sig",
        help="The encoding to use when reading the input file.  Default is "
        "UTF-8 with an optional byte-order-mark (this is what Excel writes "
        "when you ask it to save as 'CSV UTF-8').",
    )
    input_group.add_argument(
        "--tsv",
        dest="separator",
        action="store_const",
        const="\t",
        default=",",
        help="File is in tab-separated rather than comma-separated format",
    )

    input_group.add_argument(
        "--text-column",
        help="Column name (or number, if --no-headers) containing the text to analyse.",
        required=True,
    )
    input_group.add_argument(
        "--no-headers",
        action="store_false",
        default=True,
        dest="has_headers",
        help="We normally assume the first row in your CSV is column headers.  If your file "
        "does not have a header row then specify this option, in which case --text-column "
        "and --copy-columns should give the column number counting from 1, not the column "
        "header name",
    )

    output_group = parser.add_argument_group("Output settings")
    output_group.add_argument("--out", dest="out_file", required=True, help="The output CSV file")
    output_group.add_argument(
        "--out-encoding", default="utf-8", help="The encoding to use when writing output file. Default is UTF-8."
    )
    output_group.add_argument(
        "--copy-columns",
        nargs="*",
        default=[],
        help="Names of columns to copy from the input file to the output file. "
        "If --no-headers is set, the values should instead be column numbers counting "
        "from 1.",
    )
    output_group.add_argument(
        "--results",
        nargs="+",
        default=[],
        required=True,
        help="Definitions of columns to generate from the annotations returned by "
        "the GATE Cloud service.  See README for full details of the format.",
    )

    args = parser.parse_args()

    if args.api_key and args.api_password:
        api_key = args.api_key
        api_password = args.api_password

    credentials = f"{api_key}:{api_password}" if api_key and api_password else None

    processor = CsvProcessor(args, credentials)
    processor.run(args.in_file, args.encoding, args.out_file, args.out_encoding)


if __name__ == "__main__":
    main()
