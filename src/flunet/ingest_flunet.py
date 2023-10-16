import asyncio
from network.sempahore import handle_concurrency
from src.flunet.valid_flunet import valid_flunet, split_influenza_type

HUMAN_TAXID = 9606
INFA_TAXID = 11320
INFB_TAXID = 11520
QUERY = "./src/flunet/flunet.cypher"


def append_geoname(processed: dict, geonames: dict) -> None:
    territory = processed["Territory"]
    processed["geonames"] = geonames[territory]


async def ingest_flunet(
    database_handler, geoapi, ncbiapi, batch_size: int = 1000, query_path=QUERY
) -> None:
    flunet, geonames, geoids = valid_flunet(geoapi)
    geoids = await asyncio.gather(*geoids)

    geos = dict(zip(geonames, geoids))
    for row in flunet:
        append_geoname(row, geos)

    infa, infb = split_influenza_type(flunet)

    batches_infa = (len(infa) - 1) // batch_size + 1
    for i in range(batches_infa):
        batch = infa[i * batch_size : (i + 1) * batch_size]
        await database_handler.execute_query(query_path, properties=batch)

    batches_infb = (len(infb) - 1) // batch_size + 1
    for i in range(batches_infb):
        batch = infa[i * batch_size : (i + 1) * batch_size]
        await database_handler.execute_query(query_path, properties=batch)

    geoids = [x for x in geoids if x is not None]
    hierarchies = await handle_concurrency(
        *[geoapi.search_hierarchy(geoid["geonameId"]) for geoid in geoids]
    )

    await handle_concurrency(
        *[database_handler.build_geohierarchy(hierarchy) for hierarchy in hierarchies]
    )

    await handle_concurrency(
        *[
            ncbiapi.get_metadata(HUMAN_TAXID),
            ncbiapi.get_metadata(INFA_TAXID),
            ncbiapi.get_metadata(INFB_TAXID),
        ]
    )

    await handle_concurrency(
        *[database_handler.build_ncbi_hierarchy(hierarchy) for hierarchy in hierarchies]
    )
