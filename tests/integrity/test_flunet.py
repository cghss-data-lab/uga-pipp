import pytest
from network.neo4j_handler import Neo4jHandler


@pytest.fixture(scope="module")
def neo4j_handler():
    return Neo4jHandler()

@pytest.mark.asyncio
@pytest.mark.flunet
@pytest.mark.integrity
async def test_flunet_no_single_nodes(neo4j_handler):
    query = """
    MATCH (f:FluNet)
    WHERE NOT (f)-[:REPORTS]-(t)
    RETURN f
    """
    result = neo4j_handler.run_query(query)
    assert len(result) == 0

@pytest.mark.asyncio
@pytest.mark.flunet
@pytest.mark.integrity
async def test_flunet_schema(neo4j_handler):
    query = """
    MATCH (f:FluNet)-[:REPORTS]-(e:Event)-[s]-(t)
    WITH f, e, COLLECT(s) AS m, COLLECT(t) AS p
    RETURN f, e, m, p
    """
    result = neo4j_handler.run_query(query)
    assert