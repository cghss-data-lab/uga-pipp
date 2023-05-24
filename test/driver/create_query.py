def create_query_line_data(source: str, row_number: int) -> str:
    # Query node by row number, return node and first order relationships including nodes
    # The query should match the row data
    query = f"MATCH(n)-[r]-(b) WHERE n:{source} AND n:CaseReport AND n.dataSourceRow = {row_number} RETURN n, r, b, type(r)"
    return query


def count_nodes(source: str) -> str:
    query = f"MATCH(n:{source}) RETURN n.dataSourceRow"
    return query


def count_row_numbers(source: str) -> str:
    query = f" MATCH (n:{source}) WITH n.dataSourceRow as row, count(*) as count RETURN row, count"
    return query


def count_taxons(source: str, row_number: int) -> str:
    query = f"MATCH (g:{source})-[:REPORTS]->(t:Taxon) WHERE g.dataSourceRow = {row_number} RETURN count(t)"
    return query
