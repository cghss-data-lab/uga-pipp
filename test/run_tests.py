import os
from dotenv import load_dotenv
from test_flunet.test import validate_flunet
from test_gmpd.test import validate_gmpd
from driver.neo4j_driver import Neo4jDatabase

load_dotenv()

URI = os.environ["URI"]
AUTH = os.environ["AUTH"]
PASSWORD = os.environ["PASSWORD"]
DATABASE = os.environ["DATABASE"]

neo4j_connection = Neo4jDatabase(URI, DATABASE, AUTH, PASSWORD)


if __name__ == "__main__":
    validate_flunet(neo4j_connection)
    validate_gmpd(neo4j_connection)

    neo4j_connection.close()
