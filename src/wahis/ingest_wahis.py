from src.wahis.valid_wahis import valid_wahis
from network.handle_concurrency import handle_concurrency


QUERY = "src/wahis/wahis.cypher"


async def ingest_wahis(
    database_handler,
    geoapi,
    ncbiapi,
    batch_size=1000,
    query_path=QUERY,
) -> None:
    wahis, lat_long, taxons = await valid_wahis(geoapi, ncbiapi)

    tax_hierarchies = await handle_concurrency(
        *[ncbiapi.search_hierarchy(taxon) for taxon in taxons]
    )

    geo_hierarchies = await handle_concurrency(
        *[geoapi.search_hierarchy(geoid) for geoid in lat_long]
    )

    batches = (len(wahis) - 1) // batch_size + 1
    for i in range(batches):
        batch = wahis[i * batch_size : (i + 1) * batch_size]
        await database_handler.execute_query(query_path, properties=batch)

    await handle_concurrency(
        *[
            database_handler.build_geohierarchy(hierarchy)
            for hierarchy in geo_hierarchies
        ]
    )

    await handle_concurrency(
        *[
            database_handler.build_ncbi_hierarchy(hierarchy)
            for hierarchy in tax_hierarchies
        ]
    )
