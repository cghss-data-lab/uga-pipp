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
    result = neo4j_handler.run_query(query)
    assert len(result) == 0


@pytest.mark.asyncio
@pytest.mark.virion
@pytest.mark.integrity
async def test_virion_schema(neo4j_handler):
    query = """
    MATCH (v:Virion)-[s]-(t)
    WITH v, COLLECT(s) AS m, COLLECT(t) AS p
    RETURN v, m, p
    """
    result = neo4j_handler.run_query(query)

    for graph in result:
        for rel, node in zip(graph["m"], graph["p"]):
            errors = []

            if rel["type"] == "ASSOCIATES" and node["labels"] != "Taxon":
                errors.append("associates fails")

            assert len(errors) == 0
