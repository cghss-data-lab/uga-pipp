import os
from dotenv import load_dotenv
from neo4j_driver import Neo4jDatabase

load_dotenv()

URI = os.environ["URI"]
AUTH = os.environ["AUTH"]
PASSWORD = os.environ["PASSWORD"]
DATABASE = os.environ["DATABASE"]

neo4j_connection = Neo4jDatabase(URI, DATABASE, AUTH, PASSWORD)

# ingest_gmpd(SESSION)
# ingest_flunet(SESSION)
# ingest_worldpop(SESSION)


def create_query_line_data(csv_row: str) -> str:
    return query


def test_flunet_line_data() -> None:
    return


if __name__ == "__main__":
    flunet_to_ncbi = {}
    with open("./flunet/data/flunet_to_ncbi.csv", "r") as flunet_ncbi:
        for record in flunet_ncbi:
            key, value = record.split(",")
            flunet_to_ncbi[key] = value

    with open("./flunet/data/flunet_1995_2022.csv", "r") as flunet:
        header = next(flunet)  # remove header
        for row in flunet:
            query = create_query_line_data
