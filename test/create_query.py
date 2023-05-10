def create_query_line_data(source: str, row_number: int) -> str:
    # Query node by row number, return node and first order relationships including nodes
    # The query should match the row data
    query = f"MATCH(n)-[r]-(b) WHERE n:{source} AND n:CaseReport AND n.dataSourceRow = {row_number} RETURN n, r, b, type(r)"
    return query
