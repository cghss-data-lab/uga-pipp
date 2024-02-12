from devtools import debug
from loguru import logger
from tests.timer import timer
from network.handle_concurrency import handle_concurrency

from src.bvbrc_surveillance.valid_bvbrc_surveillance import valid_bvbrc_surveillance


@timer
async def ingest_bvbrc_surveillance(
    database_handler,
    geonames_api,
    ncbi_api,
    batch_size=1000,
    query_path="src/virion/bvbrc_surveillance.cypher",
) -> None:
    logger.info("Ingesting BVBRC Surveillance")
    bvbrc_valid, geonames, geonames_id, tax_names, tax_id = valid_bvbrc_surveillance(
        ncbi_api=ncbi_api, geonames_api=geonames_api
    )

    taxids = await handle_concurrency(*tax_id, n_semaphore=1)
    ncbi_hierarchies = await handle_concurrency(
        *[ncbi_api.search_hierarchy(tax) for tax in taxids], n_semaphore=1
    )

    geoids = [x for x in geonames_id if x is not None]
    geoids = await handle_concurrency(*geoids, n_semaphore=5)
    geo_hierarchies = await handle_concurrency(
        *[geonames_api.search_hierarchy(geoid["geonameId"]) for geoid in geoids]
    )

    # debug(bvbrc_valid)
    # debug(geonames)
    # debug(geo_hierarchies)
    # debug(tax_names)
    # debug(ncbi_hierarchies)

    await handle_concurrency(
        *[
            database_handler.build_geohierarchy(hierarchy)
            for hierarchy in geo_hierarchies
        ],
        n_semaphore=1
    )

    await handle_concurrency(
        *[
            database_handler.build_ncbi_hierarchy(hierarchy)
            for hierarchy in ncbi_hierarchies
        ],
        n_semaphore=1
    )
