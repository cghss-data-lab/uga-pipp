from cache.timer import timer
from src.worldpop.valid_worldpop import valid_worldpop
from network.handle_concurrency import handle_concurrency


@timer
async def ingest_worldpop(
    database_handler, geoapi, batch_size=1000, query_path="src/worldpop/worldpop.cypher"
) -> None:
    worldpop, iso_codes, geoids = valid_worldpop(geoapi)

    geoids = await handle_concurrency(*geoids, n_semaphore=5)
    isos = dict(zip(iso_codes, geoids))

    for row in worldpop:
        row["geonames"] = isos[row["ISO2_code"]]

    batches = (len(worldpop) - 1) // batch_size + 1
    for i in range(batches):
        batch = worldpop[i * batch_size : (i + 1) * batch_size]
        await database_handler.execute_query(query_path, properties=batch)

    geoids = [geoid for geoid in geoids if geoid is not None]
    geo_hierarchies = await handle_concurrency(
        *[geoapi.search_hierarchy(geoid["geonameId"]) for geoid in geoids]
    )

    await handle_concurrency(
        *[
            database_handler.build_geohierarchy(hierarchy)
            for hierarchy in geo_hierarchies
        ]
    )
