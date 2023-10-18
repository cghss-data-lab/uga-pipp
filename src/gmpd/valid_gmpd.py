import re
from loguru import logger


def is_float(number: str) -> bool:
    try:
        float(number)
    except ValueError:
        return False
    return True


def is_valid_report(row: dict) -> bool:
    if not is_float(row["Prevalence"]) or not is_float(row["NumSamples"]):
        return False
    return True


def extract_row(string: str) -> list:
    row_data = re.findall(r"(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", string)
    return row_data


def valid_gmpd(geoapi, ncbi_api, file: str = "data/GMPD_main.csv") -> list[dict]:
    geonames = set()
    tax_names = set()

    with open(file, "r", encoding="utf-8-sig") as gmpd:
        gmpd_valid = []
        header = next(gmpd)
        header = header.strip().split(",")
        for row in gmpd:
            row = extract_row(row.strip())
            data = dict(zip(header, row))

            logger.info(f"Processing row {data['dataSourceRow']}")

            if not is_valid_report(data):
                continue

            data["Positive"] = float(data["Prevalence"]) * float(data["NumSamples"])
            locations = (data["Latitude"], data["Longitude"])
            data["LocationPoint"] = locations

            geonames.add(data["LocationName"])
            tax_names.add(data["HostCorrectedName"])

            gmpd_valid.append(data)

        geonames_id = [geoapi.search_lat_long(territory) for territory in geonames]
        tax_id = [ncbi_api.search_id(tax) for tax in tax_names]

    return gmpd_valid, geonames, geonames_id, tax_names, tax_id
