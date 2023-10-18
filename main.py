import asyncio
from network.neo4j_handler import Neo4jHandler
from network.geo_api import GeonamesApi
from network.ncbi_api import NCBIApi
from src.flunet.ingest_flunet import ingest_flunet
from src.gmpd.ingest_gmpd import ingest_gmpd
from src.combine.ingest_combine import ingest_combine


async def main() -> None:
    database_handler, geonames_api, ncbi_api = Neo4jHandler(), GeonamesApi(), NCBIApi()

    # await ingest_flunet(database_handler, geonames_api, ncbi_api)
    await ingest_gmpd(database_handler, geonames_api, ncbi_api)
    await ingest_combine(database_handler)


if __name__ == "__main__":
    asyncio.run(main())
