from src.wahis.valid_wahis import valid_wahis


async def ingest_wahis(database_handler, geoapi, ncbiapi) -> None:
    wahis_valid, lat_long = await valid_wahis(geoapi, ncbiapi)
