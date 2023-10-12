import asyncio
from loguru import logger
from src.flunet.process_flunet import valid_flunet, split_influenza_type

QUERY = "./src/flunet/flunet.cypher"


def append_geoname_id(processed: dict, geonames: dict) -> dict:
    territory = processed["Territory"]
    processed["geonameId"] = geonames[territory]
    return processed


async def ingest_flunet(geoapi, file=DATASET) -> list:
    flunet, geonames, geoids = valid_flunet(geoapi)
    geoids = await asyncio.gather(*geoids)
    geos = dict(zip(geonames, geoids))
    mapping = [append_geoname_id(row, geos) for row in flunet]
    infa, infb = split_influenza_type(mapping)

    return infa, infb, geoids
