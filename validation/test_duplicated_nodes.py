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
    nodes = neo4j_connection.run_query(query)
    nodes = [dict(node) for node in nodes]

    visited_nodes = set()
    unique_nodes = []
    for node in nodes:
        item = tuple(node.items())
        if item not in visited_nodes:
            visited_nodes.add(item)
            unique_nodes.append(node)

    return unique_nodes, visited_nodes
