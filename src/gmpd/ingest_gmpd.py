import asyncio
from loguru import logger
from network.sempahore import handle_concurrency
from src.gmpd.valid_gmpd import valid_gmpd

QUERY = "./src/gmpd/gmpd.cypher"


async def ingest_gmpd(
    database_handler, geoapi, ncbiapi, batch_size: int = 1000, query_path=QUERY
) -> None:
    gmpd, geonames, geoids, taxnames, taxids = valid_gmpd(geoapi, ncbiapi)

    geoids = await asyncio.gather(*geoids)
    taxids = await asyncio.gather(*taxids)

    geographies = dict(zip(geonames, geoids))
    taxons = dict(zip(taxnames, taxids))

    for row in gmpd:
        row["locations"] = [geographies[geo] for geo in row["locations"]]
        row["HostCorrectedName"] = taxons[row["HostCorrectedName"]]
