import re
from combine.extract_properties import extract_properties


def create_properties(parameters: dict) -> str:
    properties = ", ".join("{0}: ${0}".format(n) for n in parameters)
    return properties


def preprocess_biogeographical_realms(biogeographical_realms: str) -> list:
    realms = biogeographical_realms.replace('"', "").split(",")
    realms = [realm for realm in realms if realm != "NA"]
    return realms


def ingest_combine(session) -> None:
    with open("combine/data/trait_data_imputed.csv") as combine:
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
                    MERGE (t)-[:ABOUT]->(g)
                """
                session.run(add_properties_query, properties)
