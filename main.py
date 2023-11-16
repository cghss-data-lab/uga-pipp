import sys
import asyncio
from loguru import logger
from network.neo4j_handler import Neo4jHandler
from network.geo_api import GeonamesApi
from network.ncbi_api import NCBIApi
from src.flunet.ingest_flunet import ingest_flunet
from src.gmpd.ingest_gmpd import ingest_gmpd
from src.wahis.ingest_wahis import ingest_wahis
from src.combine.ingest_combine import ingest_combine
from src.virion.ingest_virion import ingest_virion
from src.worldpop.ingest_worldpop import ingest_worldpop


logger.remove(0)
logger.add(sys.stderr, level="DEBUG")


async def main() -> None:
    database_handler, geonames_api, ncbi_api = Neo4jHandler(), GeonamesApi(), NCBIApi()

    await ingest_wahis(database_handler, geonames_api, ncbi_api)
    await ingest_virion(database_handler, ncbi_api)
    await ingest_flunet(database_handler, geonames_api, ncbi_api)
    await ingest_gmpd(database_handler, geonames_api, ncbi_api)

    # Keep combine and worldpop at the end of ingestion
    await ingest_combine(database_handler)
    await ingest_worldpop(database_handler, geonames_api)


if __name__ == "__main__":
    asyncio.run(main())
