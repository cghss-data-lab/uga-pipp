import csv
from src.combine.extract_properties import extract_properties


def preprocess_biogeographical_realms(biogeographical_realms: str) -> list:
    realms = biogeographical_realms.replace('"', "").split(",")
    realms = [realm for realm in realms if realm != "NA"]
    return realms


def valid_combine() -> list:
    combine_valid = []
    with open(
        "data/combine_trait_data_imputed.csv", "r", encoding="utf-8"
    ) as combine_file:
        combine = csv.DictReader(combine_file)
        for row in combine:
            combine_row = extract_properties(row)
            combine_row["biogeographical_realm"] = preprocess_biogeographical_realms(
                row["biogeographical_realm"]
            )
            combine_valid.append(combine_row)

    return combine_valid
