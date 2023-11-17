import pytest
from network.neo4j_handler import Neo4jHandler


@pytest.fixture(scope="module")
def neo4j_handler():
    return Neo4jHandler()


@pytest.mark.asyncio
@pytest.mark.gmpd
@pytest.mark.integrity
async def test_gmpd_no_single_nodes(neo4j_handler):
    query = """
    MATCH (g:GMPD)
    WHERE NOT (g)-[:ASSOCIATES]-() OR NOT (g)-[:ABOUT]-()
    RETURN g
    """
    result = await neo4j_handler.run_query(query)
    assert len(result) == 0


@pytest.mark.asyncio
@pytest.mark.gmpd
@pytest.mark.integrity
async def test_gmpd_schema(neo4j_handler):
    query = """
    MATCH (g:GMPD)-[r]-(t)
    WITH g, COLLECT(type(r)) AS m, COLLECT(labels(t)) AS p
    RETURN g, m, p
    """
    result = await neo4j_handler.run_query(query)

    for graph in result:
        for rel, node in zip(graph["m"], graph["p"]):
            errors = []

            if rel == "ASSOCIATES" and node[0] != "Taxon":
                errors.append("associates fails")

            if rel == "ABOUT" and node[0] != "Geography":
                errors.append("about fails")

            assert len(errors) == 0
