from network.handle_concurrency import handle_concurrency
from src.gmpd.valid_gmpd import valid_gmpd

QUERY = "./src/gmpd/gmpd.cypher"


def process_taxon(name: str, mapping: dict):
    if not mapping[name]:
        return None
    return mapping[name][-1]


async def ingest_gmpd(
    database_handler, geoapi, ncbiapi, batch_size: int = 1000, query_path=QUERY
) -> None:
    gmpd, geonames, geoids, taxnames, taxids = valid_gmpd(geoapi, ncbiapi)

    geoids = await handle_concurrency(*geoids, n_semaphore=5)

    taxids = await handle_concurrency(*taxids, n_semaphore=1)
    ncbi_hierarchies = await handle_concurrency(
        *[ncbiapi.search_hierarchy(tax) for tax in taxids], n_semaphore=1
    )

    geographies = dict(zip(geonames, geoids))
    taxons = dict(zip(taxnames, ncbi_hierarchies))

    for row in gmpd:
        row["location"] = geographies[(row["Latitude"], row["Longitude"])]
        row["Host"] = process_taxon(row["HostCorrectedName"], taxons)
        row["Parasite"] = process_taxon(row["ParasiteCorrectedName"], taxons)

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

    await handle_concurrency(
        *[
            database_handler.build_ncbi_hierarchy(hierarchy)
            for hierarchy in ncbi_hierarchies
        ]
    )
