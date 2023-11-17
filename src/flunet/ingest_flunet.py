import asyncio
from loguru import logger
from tests.timer import timer
from network.handle_concurrency import handle_concurrency
from src.flunet.valid_flunet import valid_flunet

HUMAN_TAXID = 9606
INFA_TAXID = 11320
INFB_TAXID = 11520


def append_geoname(processed: dict, geonames: dict) -> None:
    territory = processed["Territory"]
    processed["geonames"] = geonames[territory]


@timer
async def ingest_flunet(
    database_handler,
    geoapi,
    ncbiapi,
    batch_size: int = 10000,
    query_path="src/flunet/flunet.cypher",
) -> None:
    logger.info("Ingesting FluNet")
    flunet, geonames, geoids = valid_flunet(geoapi)
    geoids = await asyncio.gather(*geoids)

    geos = dict(zip(geonames, geoids))
    for row in flunet:
        append_geoname(row, geos)

    batches_infa = (len(flunet) - 1) // batch_size + 1
    for i in range(batches_infa):
        batch = flunet[i * batch_size : (i + 1) * batch_size]
        await database_handler.execute_query(query_path, properties=batch)

    geoids = [x for x in geoids if x is not None]
    geo_hierarchies = await handle_concurrency(
        *[geoapi.search_hierarchy(geoid["geonameId"]) for geoid in geoids]
    )

    await handle_concurrency(
        *[
            database_handler.build_geohierarchy(hierarchy)
            for hierarchy in geo_hierarchies
        ]
    )

    ncbi_hierarchies = await handle_concurrency(
        *[
            ncbiapi.search_hierarchy(HUMAN_TAXID),
            ncbiapi.search_hierarchy(INFA_TAXID),
            ncbiapi.search_hierarchy(INFB_TAXID),
        ]
    )

    await handle_concurrency(
        *[
            database_handler.build_ncbi_hierarchy(hierarchy)
            for hierarchy in ncbi_hierarchies
        ]
    )
