import os
from dotenv import load_dotenv
from neo4j_driver import Neo4jDatabase

load_dotenv()

URI = os.environ["URI"]
AUTH = os.environ["AUTH"]
PASSWORD = os.environ["PASSWORD"]
DATABASE = os.environ["DATABASE"]

neo4j_connection = Neo4jDatabase(URI, DATABASE, AUTH, PASSWORD)

flunet_to_ncbi = {}
with open("./flunet/data/flunet_to_ncbi.csv", "r") as flunet_ncbi:
    for record in flunet_ncbi:
        key, value = record.split(",")
        flunet_to_ncbi[key] = value


def create_query_line_data(row_number: int) -> str:
    # Query node by row number, return node and first order relationships including nodes
    # The wuery should match the row data
    query = f"MATCH(n)-[r]-(b) WHERE n:FluNet AND n:CaseReport AND n.dataSourceRow = {row_number} RETURN n, r, b, type(r)"
    return query


def test_flunet_line_data() -> None:
    return


if __name__ == "__main__":

    with open("./flunet/data/flunet_1995_2022.csv", "r") as flunet:
        header = next(flunet)  # remove header
        for row in flunet:
            query = create_query_line_data
