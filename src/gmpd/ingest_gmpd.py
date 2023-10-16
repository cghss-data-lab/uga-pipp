import asyncio
from loguru import logger
from network.sempahore import handle_concurrency
from src.gmpd.valid_gmpd import valid_gmpd

QUERY = "./src/gmpd/gmpd.cypher"


def process_geographies(name: str, geographies: dict) -> dict:
    geo = {"name": name, "geonameId": geographies[name]}
    return geo


def process_taxon(name: str, taxons: dict) -> dict:
    tax = {"name": name, "taxId": taxons[name]}
    return tax


async def ingest_gmpd(
    database_handler, geoapi, ncbiapi, batch_size: int = 1000, query_path=QUERY
) -> None:
    gmpd, geonames, geoids, taxnames, taxids = valid_gmpd(geoapi, ncbiapi)

    geoids = await asyncio.gather(*geoids)
    taxids = await asyncio.gather(*taxids)

    geographies = dict(zip(geonames, geoids))
    taxons = dict(zip(taxnames, taxids))

    for row in gmpd:
        row["locations"] = [
            process_geographies(geo, geographies) for geo in row["locations"]
        ]
        row["HostCorrectedName"] = process_taxon(row["HostCorrectedName"], taxons)
