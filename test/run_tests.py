import os
import logging
from dotenv import load_dotenv
from test_flunet.test import validate_flunet
from test_gmpd.test import validate_gmpd
from test_global.test import validate_row_duplicates, validate_node_duplicates
from driver.neo4j_driver import Neo4jDatabase
from driver.errors import DuplicationError

load_dotenv()

URI = os.environ["URI"]
AUTH = os.environ["AUTH"]
PASSWORD = os.environ["PASSWORD"]
DATABASE = os.environ["DATABASE"]

neo4j_connection = Neo4jDatabase(URI, DATABASE, AUTH, PASSWORD)


if __name__ == "__main__":

    # Validate FluNet
    validate_flunet(neo4j_connection)
    try:
        validate_node_duplicates(neo4j_connection, "FluNet")
    except DuplicationError as e:
        logging.error("Error in %s", exc_info=e)

    validate_gmpd(neo4j_connection)

    neo4j_connection.close()
