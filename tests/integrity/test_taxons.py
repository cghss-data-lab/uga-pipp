import pytest
from network.neo4j_handler import Neo4jHandler


@pytest.fixture
def neo4j_handler():
    return Neo4jHandler()


async def test_duplicate_geographies(neo4j_handler):
    query = """
    MATCH (t1:Taxon)
    MATCH (t2:Taxon)
    WHERE id(t1) <> id(t2) AND properties(t1) = properties(t2)
    RETURN g1, g2)
    """
    result = await neo4j_handler.run_query(query)
    assert len(result) == 0


async def test_unique_geoname_id(neo4j_handler):
    query = """
    MATCH (t:Taxon)
    RETURN t.taxId
    """
    result = await neo4j_handler.run_query(query)
    assert len(result) == len(set(result))
