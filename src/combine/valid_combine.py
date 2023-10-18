import re
from loguru import logger
from src.combine.extract_properties import extract_properties


def preprocess_biogeographical_realms(biogeographical_realms: str) -> list:
    realms = biogeographical_realms.replace('"', "").split(",")
    realms = [realm for realm in realms if realm != "NA"]
    return realms


def valid_combine() -> list:
    combine_valid = []
    with open("data/combine_trait_data_imputed.csv", "r", encoding="utf-8") as combine:
        header = next(combine).replace("-", "_").strip().split(",")
        for line in combine:
            line = line.strip()
            row_data = re.split(r"(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", line)
            row_data = [data for data in row_data if data != ""]
            data = dict(zip(header, row_data))
            combine_row = extract_properties(data)
            combine_row["biogeographical_realm"] = preprocess_biogeographical_realms(
                data["biogeographical_realm"]
            )
            combine_valid.append(combine_row)

    return combine_valid
