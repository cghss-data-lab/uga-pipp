import asyncio
from src.flunet.process_flunet import valid_flunet, split_influenza_type

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

    tasks = []

    batches_infa = (len(infa) - 1) // batch_size + 1
    for i in range(batches_infa):
        batch = infa[i * batch_size : (i + 1) * batch_size]
        tasks.append(await database_handler.execute_query(QUERY, properties=batch))

    batches_infb = (len(infb) - 1) // batch_size + 1
    for i in range(batches_infb):
        batch = infa[i * batch_size : (i + 1) * batch_size]
        tasks.append(await database_handler.execute_query(QUERY, properties=batch))

    geoids = [x for x in geoids if x is not None]
    hierarchies = [
        await geoapi.search_hierarchy(geoid["geonameId"]) for geoid in geoids
    ]
    tasks.extend(hierarchies)

    # tasks.extend(
    #     [
    #         await database_handler.merge_geo(geoid, hierarchy)
    #         for geoid, hierarchy in zip(geoids, hierarchies)
    #     ]
    # )

    return tasks
