import base64
import sys

import requests


def main():
    api_key = None
    api_password = None
    try:
        import settings

        api_key = settings.API_KEY
        api_password = settings.API_PASSWORD
    except:
        # Ignore settings file
        pass

    endpoint = sys.argv[1]
    if endpoint.startswith("https://cloud-api.gate.ac.uk"):
        # This is a standard GATE Cloud endpoint, so first try the sheets services file
        with requests.get("https://github.com/GateNLP/gate-metadata/raw/refs/heads/master/sheets-services/services-1.0.0.json") as resp:
            resp.raise_for_status()
            services = resp.json()
            _, _, slug = endpoint.rpartition("/")
            service_info = next((s for s in services if s["slug"] == slug), None)
            if service_info:
                print(f"Service {slug} has the following recommended result specifiers:")
                print()
                for spec in service_info["configs"].get("standard", []):
                    print(f"    {spec}")
                if "extra" in service_info["configs"]:
                    print()
                    print("and the following additional examples:")
                    print()
                    for spec in service_info["configs"]["extra"]:
                        print(f"    {spec}")

                sys.exit(0)

    # We didn't find any info in the sheets services file, so try the service metadata
    headers = {"Accept": "application/json"}
    if api_key:
        auth_header = "Basic " + base64.b64encode(bytes(f"{api_key}:{api_password}", "utf-8")).decode("ascii")
        headers["Authorization"] = auth_header
    with requests.get(endpoint + "/metadata", headers=headers) as resp:
        resp.raise_for_status()
        service_metadata = resp.json()
        all_selectors = []
        if service_metadata.get("defaultAnnotations"):
            all_selectors.extend(sel.strip() for sel in service_metadata["defaultAnnotations"].split(","))
        if service_metadata.get("additionalAnnotations"):
            all_selectors.extend(sel.strip() for sel in service_metadata["additionalAnnotations"].split(","))
        all_types = set()
        for sel in all_selectors:
            _, _, ann_type = sel.partition(":")
            all_types.add(ann_type)

    print(f"Service {endpoint} can return the following annotation types:")
    print()
    for t in all_types:
        print(f"    {t}")
    print()
    print("For details on the features of each annotation, see the documentation or try the")
    print("service yourself with some sample data.")


if __name__ == "__main__":
    main()