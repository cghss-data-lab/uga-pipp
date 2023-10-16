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


def process_geographies(geographies: str) -> list[str]:
    geographies = geographies.strip('"')
    geographies = re.split(r",\s*|\sand\s*", geographies)
    return [geo.lower() for geo in geographies]


def valid_gmpd(geoapi, ncbi_api, file: str = "/data/GMPD_main.csv") -> list[dict]:
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
            locations = process_geographies(data["LocationName"])
            data["LocationName"] = locations

            geonames.update(locations)
            tax_names.add(data["HostCorrectedName"])

            gmpd_valid.append(data)

        geonames_id = [geoapi.search_geonameid(territory) for territory in geonames]
        tax_id = [ncbi_api.search_id(tax) for tax in tax_names]

    return gmpd_valid, geonames, geonames_id, tax_names, tax_id
