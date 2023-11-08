import pytest
from network.neo4j_handler import Neo4jHandler


@pytest.fixture
def neo4j_handler():
    return Neo4jHandler()
