import os
from dotenv import load_dotenv
from test_flunet.validation import flunet_validation, flunet_count
from test_gmpd.validation import gmpd_count
from driver.neo4j_driver import Neo4jDatabase

load_dotenv()

URI = os.environ["URI"]
AUTH = os.environ["AUTH"]
PASSWORD = os.environ["PASSWORD"]
DATABASE = os.environ["DATABASE"]

neo4j_connection = Neo4jDatabase(URI, DATABASE, AUTH, PASSWORD)


def test_flunet_count():
    assert flunet_count(neo4j_connection) == 0


def test_flunet_accuracy():
    assert flunet_validation(neo4j_connection) == 0


def test_gmpd_count():
    assert gmpd_count(neo4j_connection) == 0


def test_gmpd_accuracy():
    assert gmpd_validation() == 0


neo4j_connection.close()

# if __name__ == "__main__":
#    flunet_count(neo4j_connection)

#    neo4j_connection.close()
