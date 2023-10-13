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
    return re.split(r",\s*|and\s*", geographies)


def valid_gmpd(file: str = "/data/GMPD_main.csv") -> list[dict]:
    with open(file, "r", encoding="utf-8-sig") as gmpd:
        gmpd_valid = []
        header = next(gmpd)
        header = header.strip().split(",")
        for row in gmpd:
            row = extract_row(row.strip())
            data = dict(zip(header, row))
            logger.info(f"Processing row {data['dataSourceRow']}")
            if is_valid_report(data):
                data["Positive"] = float(data["Prevalence"]) * float(data["NumSamples"])
                data["LocationName"] = process_geographies(data["LocationName"])
                gmpd_valid.append(data)
    return gmpd_valid
