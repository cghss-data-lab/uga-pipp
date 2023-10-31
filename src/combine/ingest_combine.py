from src.combine.valid_combine import valid_combine


async def ingest_combine(
    database_handler, batch_size: int = 1000, query_path="src/combine/combine.cypher"
) -> None:
    combine = valid_combine()

    batches = (len(combine) - 1) // batch_size + 1
    for i in range(batches):
        batch = combine[i * batch_size : (i + 1) * batch_size]
        await database_handler.execute_query(query_path, properties=batch)
