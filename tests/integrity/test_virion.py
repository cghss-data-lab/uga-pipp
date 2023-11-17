import pytest
from network.neo4j_handler import Neo4jHandler


@pytest.fixture(scope="module")
def neo4j_handler():
    return Neo4jHandler()


@pytest.mark.asyncio
@pytest.mark.virion
@pytest.mark.integrity
async def test_virion_no_single_nodes(neo4j_handler):
    query = """
    MATCH (v:Virion)
    WHERE NOT (v)-[:ASSOCIATES]-()
    RETURN v
    """
    result = await neo4j_handler.run_query(query)
    assert len(result) == 0


@pytest.mark.asyncio
@pytest.mark.virion
@pytest.mark.integrity
async def test_virion_schema(neo4j_handler):
    query = """
    MATCH (v:Virion)-[s]-(t)
    WITH v, COLLECT(type(s)) AS m, COLLECT(labels(t)) AS p
    RETURN v, m, p
    """
    result = await neo4j_handler.run_query(query)

    for graph in result:
        for rel, node in zip(graph["m"], graph["p"]):
            errors = []

            if rel == "ASSOCIATES" and node[0] != "Taxon":
                errors.append("associates fails")

            assert len(errors) == 0
