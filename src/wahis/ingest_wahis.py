from src.wahis.valid_wahis import valid_wahis
from network.handle_concurrency import handle_concurrency


async def ingest_wahis(database_handler, geoapi, ncbiapi) -> None:
    wahis_valid, lat_long = await valid_wahis(geoapi, ncbiapi)

    geo_hierarchy = handle_concurrency(
        *[geoapi.search_hierarchy(geoid) for geoid in lat_long]
    )
