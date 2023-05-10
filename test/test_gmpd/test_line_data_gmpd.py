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


if __name__ == "__main__":
    pass
