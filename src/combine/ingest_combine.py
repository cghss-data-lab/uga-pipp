from loguru import logger
from tests.timer import timer
from src.combine.valid_combine import valid_combine


@timer
async def ingest_combine(
    database_handler, batch_size: int = 1000, query_path="src/combine/combine.cypher"
) -> None:
    logger.info("Ingesting Combine")
    combine = valid_combine()

    batches = (len(combine) - 1) // batch_size + 1
    for i in range(batches):
        batch = combine[i * batch_size : (i + 1) * batch_size]
        await database_handler.execute_query(query_path, properties=batch)
