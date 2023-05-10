import os
from dotenv import load_dotenv
from neo4j_driver import Neo4jDatabase


load_dotenv()

URI = os.environ["URI"]
AUTH = os.environ["AUTH"]
PASSWORD = os.environ["PASSWORD"]
DATABASE = os.environ["DATABASE"]

neo4j_connection = Neo4jDatabase(URI, DATABASE, AUTH, PASSWORD)


def create_query_node_labels(labels: list) -> str:
    # Creates a query that returns every memeber of a node type
    where_clause = " AND ".join(["n:" + l for l in labels])
    query = f"MATCH (n) WHERE {where_clause} RETURN n"
    return query


def test_duplicate_nodes(query: str) -> None:
    # Visit all nodes and verify if there are duplicates
    nodes = neo4j_connection.run_query(query)
    nodes = [dict(node) for node in nodes]

    visited_nodes = set()
    unique_nodes = []
    for node in nodes:
        # Transoforms the dictionary into a tuple of tuples,
        # each element is (key, value) from the dictionary
        item = tuple(sorted(node.items()))
        if item not in visited_nodes:
            visited_nodes.add(item)
            unique_nodes.append(node)

    return unique_nodes, visited_nodes


if __name__ == "__main__":

    # Find all node types based on labels
    node_labels_count = neo4j_connection.run_query(
        """MATCH (n) RETURN distinct labels(n)"""
    )

    # From each node type, extract labels and verify if there are duplicates
    for node in node_labels_count:
        node_type_labels = dict(node)["labels(n)"]
        node_type_query = create_query_node_labels(node_type_labels)
        unique, visited = test_duplicate_nodes(node_type_query)

        test_status = len(unique) == len(visited)

        # Print stauts for each node type
        print(
            node_type_labels,
            "Unique: ",
            len(unique),
            "Visited: ",
            len(visited),
            "Status: ",
            test_status,
        )
