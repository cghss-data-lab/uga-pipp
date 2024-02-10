import csv
from datetime import datetime
from loguru import logger


def parse_and_format_date(date: str) -> str | None:
    try:
        date_strip = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
        return date_strip.strftime("%Y-%m-%d")
    except ValueError:
        logger.trace("Malformed date: '{}'", date)
        return None


def valid_bvbrc_surveillance(
    geonames_api, ncbi_api, file: str = "data/BVBRC_surveillance.csv"
):
    geonames = set()
    tax_names = set()
    bvbrc_valid = []

    with open(file, "r", encoding="utf-8-sig") as bvbrc_file:
        bvbrc = csv.DictReader(bvbrc_file)
        for idx, row in enumerate(bvbrc):
            if idx > 50:
                break

            row["collection_date"] = parse_and_format_date(row["Collection Date"])

            if row["Collection Latitude"] and row["Collection Longitude"]:
                row["location"] = (
                    row["Collection Latitude"],
                    row["Collection Longitude"],
                )
                geonames.add(row["location"])

            if row["Host Species"] == "Env":
                row["host_search_name"] = row["Host Group"]
            elif row["Host Species"]:
                row["host_search_name"] = row["Host Species"]
            elif row["Host Common Name"]:
                row["host_search_name"] = row["Host Common Name"]
            else:
                row["host_search_name"] = row["Host Group"]

            # row["Positive"] = float(row["Prevalence"]) * float(row["NumSamples"])
            # locations = (row["Latitude"], row["Longitude"])

            tax_names.add(row["host_search_name"])

            # tax_names.add(row["HostCorrectedName"])
            # tax_names.add(row["ParasiteCorrectedName"])

            bvbrc_valid.append(row)

        geonames_id = [geonames_api.search_lat_long(place) for place in geonames]
        tax_id = [ncbi_api.search_id(tax) for tax in tax_names]

    return bvbrc_valid, geonames, geonames_id, tax_names, tax_id
