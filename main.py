import asyncio
from network.neo4j_handler import Neo4jHandler
from network.geo_api import GeonamesApi
from network.ncbi_api import NCBIApi
from src.flunet.ingest_flunet import ingest_flunet


async def main() -> None:
    database_handler, geonames_api, ncbi_api = Neo4jHandler(), GeonamesApi(), NCBIApi()

    await ingest_flunet(database_handler, geonames_api, ncbi_api)


if __name__ == "__main__":
    asyncio.run(main())
