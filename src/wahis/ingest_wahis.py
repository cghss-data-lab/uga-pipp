from loguru import logger
from tests.timer import timer
from src.wahis.valid_wahis import valid_wahis
from network.handle_concurrency import handle_concurrency


@timer
async def ingest_wahis(
    database_handler,
    geoapi,
    ncbiapi,
    batch_size=1000,
    query_path="src/wahis/wahis.cypher",
) -> None:
    logger.info("Ingesting WAHIS")
    wahis, geonames, tax_names, tax_ids = await valid_wahis(geoapi, ncbiapi)

    tax_hierarchies = await handle_concurrency(
        *[ncbiapi.search_hierarchy(taxon) for taxon in tax_ids]
    )

    taxons = dict(zip(tax_names, tax_hierarchies))

    for row in wahis:
        row["host"] = ncbiapi.process_taxon(row["event"]["disease"]["group"], taxons)
        row["pathogen"] = ncbiapi.process_taxon(
            row["event"]["causalAgent"]["name"], taxons
        )
        for outbreak in row["outbreaks"]:
            outbreak["geonames"] = geonames[outbreak["geonames"]]

    geo_hierarchies = await handle_concurrency(
        *[geoapi.search_hierarchy(geoid["geonameId"]) for geoid in geonames.values()]
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
