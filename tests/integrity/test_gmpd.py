import pytest
from network.neo4j_handler import Neo4jHandler


@pytest.fixture(scope="module")
def neo4j_handler():
    return Neo4jHandler()


async def test_gmpd(neo4j_handler):
    assert