import os
from dotenv import load_dotenv
import collections
from datetime import datetime
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
        value = value.strip()
        flunet_to_ncbi[value] = key


def create_query_line_data(row_number: int) -> str:
    # Query node by row number, return node and first order relationships including nodes
    # The query should match the row data
    query = f"MATCH(n)-[r]-(b) WHERE n:FluNet AND n:CaseReport AND n.dataSourceRow = {row_number} RETURN n, r, b, type(r)"
    return query


def is_flunet_node_accurate(row: dict, node: dict) -> bool:
    keys = [
        "",
        "Start date",
        "End date",
        "Collected",
        "Processed",
        "Total positive",
        "Total negative",
    ]

    node["start"] = node["start"].strftime("%m/%d/%y")

    node_dictionary = {key: row[key] for key in keys}
    start = datetime.strptime(node_dictionary["Start date"], "%m/%d/%y")
    end = datetime.strptime(node_dictionary["End date"], "%m/%d/%y")
    node_dictionary["End date"] = end - start

    node_dictionary_values = node_dictionary.values()

    return collections.Counter(node_dictionary_values) == collections.Counter(
        list(node.values())
    )


def is_strain_accurate(row: dict, strain_name: str) -> bool:
    strain = flunet_to_ncbi[strain_name]
    if row[strain] != 0:
        return True
    return False


def test_flunet_line_data(csv_row: dict, query_result: list) -> dict:
    # Verify node exists
    accuracy = {}
    is_node_checked = False
    for result in query_result:
        node, relationship, adjacent_node, type_relationship = result.values()
        # Check the primary node
        if not is_node_checked:
            node_accuracy = is_flunet_node_accurate(csv_row, dict(node))
            accuracy["node"] = node_accuracy
            is_node_checked = True
        # Check territorial scope
        if type_relationship == "IN":
            territory = csv_row["Territory"]
            territory_name = dict(adjacent_node)["name"]
            accuracy["territory"] = territory == territory_name
        # Check reported nodes
        if "host" not in relationship and type_relationship == "REPORTS":
            adj_name = adjacent_node["name"]
            adj_accuracy = is_strain_accurate(csv_row, adj_name)
            accuracy[adj_name] = adj_accuracy
    return accuracy


if __name__ == "__main__":

    with open("./flunet/data/flunet_1995_2022.csv", "r") as flunet:
        header = next(flunet).split(",")
        for row in flunet:
            row = row.split(",")
            # Create a dictionary with the line data
            row_as_dictionary = {k: v for k, v in zip(header, row)}
            query = create_query_line_data(row_as_dictionary[""])
            query_results = neo4j_connection.run_query(query)

            line_data_accuracy = test_flunet_line_data(row_as_dictionary, query_results)
