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
    result = await neo4j_handler.run_query(query)
    assert len(result) == 0


@pytest.mark.asyncio
@pytest.mark.flunet
@pytest.mark.integrity
async def test_flunet_schema(neo4j_handler):
    query = """
    MATCH (f:FluNet)-[:REPORTS]-(:Event)-[s]-(t)
    WITH f, COLLECT(s) AS m, COLLECT(t) AS p
    RETURN f, m, p
    """
    result = await neo4j_handler.run_query(query)

    for graph in result:
        for rel, node in zip(graph["m"], graph["p"]):
            errors = []

            if rel["type"] == "INVOLVES" and node["labels"] != "Taxon":
                errors.append("involves fails")

            if rel["type"] == "OCCURS_IN" and node["labels"] != "Geography":
                errors.append("occurs_in fails")

            assert len(errors) == 0
