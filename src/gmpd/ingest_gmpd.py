import asyncio
from loguru import logger
from network.handle_concurrency import handle_concurrency
from src.gmpd.valid_gmpd import valid_gmpd

QUERY = "./src/gmpd/gmpd.cypher"


async def ingest_gmpd(
    database_handler, geoapi, ncbiapi, batch_size: int = 1000, query_path=QUERY
) -> None:
    gmpd, geonames, geoids, taxnames, taxids = valid_gmpd(geoapi, ncbiapi)

    geoids = await handle_concurrency(*geoids)
    taxids = await handle_concurrency(*taxids)

    geographies = dict(zip(geonames, geoids))
    taxons = dict(zip(taxnames, taxids))

    for row in gmpd:
        row["locations"] = [geographies[geo] for geo in row["locations"]]
        row["HostCorrectedName"] = taxons[row["HostCorrectedName"]]

    batches = (len(gmpd) - 1) // batch_size + 1
    for i in range(batches):
        batch = gmpd[i * batch_size : (i + 1) * batch_size]
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

    taxids = [x for x in taxids if x is not None]
    ncbi_hierarchies = await handle_concurrency(
        *[ncbiapi.search_hierarchy(tax) for tax in taxids]
    )

    await handle_concurrency(
        *[
            database_handler.build_ncbi_hierarchy(hierarchy)
            for hierarchy in ncbi_hierarchies
        ]
    )
