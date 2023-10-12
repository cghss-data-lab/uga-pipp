import asyncio
from loguru import logger
from src.flunet.process_flunet import valid_flunet, split_influenza_type

QUERY = "./src/flunet/flunet.cypher"


def append_geoname_id(processed: dict, geonames: dict) -> dict:
    territory = processed["Territory"]
    processed["geonameId"] = geonames[territory]
    return processed


async def ingest_flunet(database_handler, geoapi, batch_size: int = 1000) -> list:
    flunet, geonames, geoids = valid_flunet(geoapi)
    geoids = await asyncio.gather(*geoids)
    geos = dict(zip(geonames, geoids))
    mapping = [append_geoname_id(row, geos) for row in flunet]
    infa, infb = split_influenza_type(mapping)

    geoids = [x for x in geoids if x is not None]

    tasks = []

    batches_infa = (len(infa) - 1) // batch_size + 1
    for i in range(batches_infa):
        batch = infa[i * batch_size : (i + 1) * batch_size]
        tasks.append(database_handler.execute_query(QUERY, properties=batch))

    batches_infb = (len(infb) - 1) // batch_size + 1
    for i in range(batches_infb):
        batch = infa[i * batch_size : (i + 1) * batch_size]
        tasks.append(database_handler.execute_query(QUERY, properties=batch))

    tasks.extend([geoapi.search_hierarchy(geoid) for geoid in geoids])
    tasks.extend([database_handler.merge_geo(geoid) for geoid in geoids])

    return tasks
