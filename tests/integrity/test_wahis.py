import pytest
from network.neo4j_handler import Neo4jHandler


@pytest.fixture(scope="module")
def neo4j_handler():
    return Neo4jHandler()


@pytest.mark.asyncio
@pytest.mark.wahis
@pytest.mark.integrity
async def test_wahis_no_single_nodes(neo4j_handler):
    query = """
    MATCH (w:WAHIS)
    WHERE NOT (w))-[:REPORTS]-(t)
    RETURN w
    """
    result = neo4j_handler.run_query(query)
    assert len(result) == 0

@pytest.mark.asyncio
@pytest.mark.wahis
@pytest.mark.integrity
async def test_wahis_schema(neo4j_handler):
    query = """
    MATCH (f:FluNet)-[:REPORTS]-(e:Event)-[s]-(t)
    WITH f, e, COLLECT(s) AS m, COLLECT(t) AS p
    RETURN f, e, m, p
    """
    result = neo4j_handler.run_query(query)
    assert