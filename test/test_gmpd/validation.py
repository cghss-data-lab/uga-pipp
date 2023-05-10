import os
from dotenv import load_dotenv
import collections
from datetime import datetime
from neo4j_driver import Neo4jDatabase
from create_query import create_query_line_data


load_dotenv()

URI = os.environ["URI"]
AUTH = os.environ["AUTH"]
PASSWORD = os.environ["PASSWORD"]
DATABASE = os.environ["DATABASE"]


neo4j_connection = Neo4jDatabase(URI, DATABASE, AUTH, PASSWORD)


def gmpd_validation() -> None:
    pass
