import pytest
from network.neo4j_handler import Neo4jHandler


@pytest.fixture
def neo4j_handler():
    return Neo4jHandler()


@pytest.mark.integrity
async def test_duplicate_geographies(neo4j_handler):
    query = """
    MATCH (g1:Geography)
    MATCH (g2:Geography)
    WHERE id(g1) <> id(g2) AND properties(g1) = properties(g2)
    RETURN g1, g2)
    """
    result = await neo4j_handler.run_query(query)
    assert len(result) == 0


@pytest.mark.integrity
async def test_unique_geoname_id(neo4j_handler):
    query = """
    MATCH (g:Geography)
    RETURN g.geonameId
    """
    result = await neo4j_handler.run_query(query)
    assert len(result) == len(set(result))
