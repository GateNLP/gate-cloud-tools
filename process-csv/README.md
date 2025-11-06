# GATE Cloud CSV Processor

This is a tool that allows you to use a [GATE Cloud](https://cloud.gate.ac.uk) service to annotate a set of text documents in a CSV file, and write the results to another CSV file.

## Setup

This tool is written in Python, and requires an installation of Python 3.11 or later.  It relies on a few third-party packages from PyPI, so the recommended approach is to create a _virtual environment_ and install the dependencies in there:

```shell
python -m venv venv
. venv/bin/activate
pip install -r requirements.txt
```

## Basic usage

Once you have activated the virtual environment and installed the requirements, you can run the tool as:

```shell
python process_csv.py --endpoint https://cloud-api.gate.ac.uk/process/annie-named-entity-recognizer \
    --in input.csv --out annie.csv --text-column text \
    --copy-columns id \
    --results "Person" "Location" "Organization"
```

## Authentication

By default, the tool will not authenticate to GATE Cloud, which means you are subject to the (very low) rate limits and quotas for unauthenticated users.  In almost all cases it is better to sign in to your GATE Cloud account and create an [API key](https://cloud.gate.ac.uk/yourAccount/apiKeys).  Create a file named `settings.py` in the same folder as `process_csv.py` with the details of your API key:

```python
API_KEY = 'your-gate-cloud-api-key-id'
API_PASSWORD = 'generated-password'
```

This will allow you to use your full account quota when calling the GATE Cloud API endpoints.

## Parameters

### The service to call

- `--endpoint` (required): the GATE Cloud service endpoint you want to use, this can be found in the "use this pipeline" section of the service detail page on https://cloud.gate.ac.uk

### Input file specification

- `--in` (required): path to the input CSV file
- `--text-column` (required): which column in the input CSV contains the text to be processed.  If your CSV file has a header row, this should be the relevant column heading; if your file does not have a header row then you should specify the column by number, counting from 1, and also specify `--no-headers`
- `--no-headers`: specify this if your CSV _does not_ have a header row, i.e. the first row of the file is real data
- `--encoding` (optional, default `utf-8-sig`): character encoding to use when reading the CSV file.  This defaults to UTF-8, and handles any initial byte-order-mark - this is the format that Microsoft Excel writes when you save as "CSV UTF-8"
- `--tsv` (optional): specify this if your input file is separated by tabs rather than commas
- `--html` (optional): specify this if the "text" in your `--text-column` is actually HTML

### Output file specification

- `--out` (required): path to the CSV file where you want to write the results
- `--out-encoding` (optional, default `utf-8`): encoding in which to write the output CSV file
- `--copy-columns` (optional): specify columns that should be copied verbatim from the input to the output file.  For example, if each row includes a document ID as well as the text, then you may want to copy that ID column into the output in order to cross-reference which output row matches which input row, or you may just want to copy the text column itself to make the output file more self-contained
- `--results` (required): one or more "result specifiers" defining how to map the response from GATE Cloud into columns in your output CSV.  The format of these is discussed in more detail below.  Any specifiers that contain spaces or characters with a special meaning to your shell _must_ be quoted; we recommend you quote all specifiers to be safe.

### Result specifiers

The `--results` option takes a series of one or more specifiers that define how to map the annotations from GATE Cloud into columns in the output CSV.  They are a kind of "controlled language" designed to be unambiguous but still human-readable.

Each specifier starts with the name of an annotation type, e.g. "Person" or "Hashtag", and there are two basic types of option:

- "Content" options that extract text or features from each annotation of that type
- "Summary" options that give a single piece of information about all annotations of that type in the text

The simplest configuration option is just the annotation type on its own, e.g. `Person` - this finds all the spans of text from the input that are annotated as this type, and returns them as a single string separated by semicolons. But this is a special case of a more general configuration of `[AnnotationType] [pattern]`. The pattern is made up of words separated by spaces or punctuation, and each word represents a _feature name_ - the special feature name `text` represents the span of text covered by the annotation (so the simple case `Person` is just shorthand for `Person text`). The pattern is filled in for each annotation using the values of the relevant features, and the resulting strings are joined by semicolons to produce the final result. This is more easily explained by examples:

- `Person` or `Person text` produces the text covered by each Person annotation, e.g. "John Brown" or "Mary"
- `Person text (gender)` adds the "gender" feature in parentheses, e.g. "John Brown (male)", "Mary (female)"
- `Mention STY` (an example from [Bio-YODIE](https://cloud.gate.ac.uk/shopfront/displayItem/bio-yodie)) extracts the "STY" feature of each Mention annotation, which in this case is the semantic type such as "Disease or Syndrome", "Phamacologic Substance", etc.
- The modifier `as %` is available for features like confidence scores that are numbers between 0 and 1, to convert the number to a percentage between 0 and 100.
    - `Veracity rumour_label (confidence as %)` (from the [rumour veracity classifier](https://cloud.gate.ac.uk/shopfront/displayItem/multilingual-rumour-veracity))
- Any feature names that contain spaces or punctuation (other than `-` or `_`) should be surrounded with square brackets in the pattern, i.e. `[some feature]`.

Sometimes you simply want to know whether or not a text contains any mentions of a given annotation type, or just the number of mentions rather than the data from each individual one. The following summary configurations are currently available:

- `AnnotationType present?` - 1 if there are any mentions of this annotation type found in the text, 0 otherwise
- `AnnotationType #count` - the number of mentions of an annotation type

Many of the standard GATE Cloud services have pre-defined standard specifiers that you can find by running

```shell
python service_details.py https://cloud-api.gate.ac.uk/process/...
```

with the appropriate API endpoint.  If the particular service you want does not have pre-defined specifiers, this command will at least show you which annotation _types_ are available; you will need to experiment with the "test this pipeline" tool on GATE Cloud to determine exactly what feature patterns are appropriate for such a service.
