from network.handle_concurrency import handle_concurrency
from src.virion.valid_virion import valid_virion


def process_taxon(name: str, mapping: dict):
    if mapping[name] is None:
        return
    return mapping[name][-1]


async def ingest_virion(
    database_handler, ncbiapi, batch_size=1000, query_path="src/virion/virion.cypher"
) -> None:
    virion, taxids, ncbi_hierarchies = valid_virion(ncbiapi)

    ncbi_hierarchies = await handle_concurrency(*ncbi_hierarchies, n_semaphore=2)

    taxons = dict(zip(taxids, ncbi_hierarchies))

    for row in virion:
        row["host"] = process_taxon(row["HostTaxID"], taxons)
        row["pathogen"] = process_taxon(row["VirusTaxID"], taxons)

    batches = (len(virion) - 1) // batch_size + 1
    for i in range(batches):
        batch = virion[i * batch_size : (i + 1) * batch_size]
        await database_handler.execute_query(query_path, properties=batch)

    ncbi_hierarchies = [ncbi for ncbi in ncbi_hierarchies if ncbi is not None]
    await handle_concurrency(
        *[
            database_handler.build_ncbi_hierarchy(hierarchy)
            for hierarchy in ncbi_hierarchies
        ]
    )
