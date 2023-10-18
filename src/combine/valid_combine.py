import re
from loguru import logger
from src.combine.extract_properties import extract_properties


def create_properties(parameters: dict) -> str:
    properties = ", ".join("{0}: ${0}".format(n) for n in parameters)
    return properties


def preprocess_biogeographical_realms(biogeographical_realms: str) -> list:
    realms = biogeographical_realms.replace('"', "").split(",")
    realms = [realm for realm in realms if realm != "NA"]
    return realms


def log_taxons_without_match(result: list, row: dict) -> None:
    if len(result) == 0:
        logger.warning(row["iucn2020_binomial"] + " doesn't have a match")


def ingest_combine(session) -> None:
    with open("data/combine_trait_data_imputed.csv", "r", encoding="utf-8") as combine:
        header = next(combine).replace("-", "_").strip().split(",")
        for line in combine:
            line = line.strip()
            row_data = re.split(r"(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", line)
            row_data = [data for data in row_data if data != ""]
            data = {key: value for key, value in zip(header, row_data)}
            data["biogeographical_realm"] = preprocess_biogeographical_realms(
                data["biogeographical_realm"]
            )
            properties = extract_properties(data)
            for realm in data["biogeographical_realm"]:
                add_properties_query = f"""
                    MATCH (t:Taxon)
                    WHERE t.name = "{data['iucn2020_binomial']}"
                    SET t += {{ {create_properties(properties)} }}
                    MERGE (g:BioGeographicalRealm:Geography {{name : "{realm}" }})
                    MERGE (t)-[:INHABITS]->(g)
                    RETURN t
                """
                result = session.run(add_properties_query, properties)
                log_taxons_without_match(list(result), data)
