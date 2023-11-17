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
    WHERE NOT (w))-[:REPORTS]-()
    RETURN w
    """
    result = await neo4j_handler.run_query(query)
    assert len(result) == 0


@pytest.mark.asyncio
@pytest.mark.wahis
@pytest.mark.integrity
async def test_wahis_schema(neo4j_handler):
    query = """
    MATCH (w:WAHIS)-[:REPORTS]-(:Event)-[s]-(t)
    WITH w, COLLECT(s) AS m, COLLECT(t) AS p
    RETURN w, m, p
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
