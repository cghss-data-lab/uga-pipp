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
from src.worldpop.ingest_worldpop import ingest_worldpop


logger.remove(0)
logger.add(sys.stderr, level="TRACE")


async def main() -> None:
    database_handler, geonames_api, ncbi_api = Neo4jHandler(), GeonamesApi(), NCBIApi()

    # # Cache to write the ncbi none items to a file
    # ncbi_none = {}
    # with open("network/cache/ncbi_id.pickle", "rb") as c:
    #     ncbi_cache = pickle.load(c)
    #     for key, value in ncbi_cache.items():
    #         if value is None:
    #             ncbi_none[key] = value
    # with open("./ncbi_none.csv", "w") as c:
    #     writer = csv.writer(c)
    #     for key, value in ncbi_none.items():
    #         writer.writerow([key, value])

    # Code to drop NONE out of the ncbi ID cache
    # so that we can use an updated synonyms map
    # new_cache = {}
    # with open("network/cache/ncbi_id.pickle", "rb") as c:
    #     ncbi_cache = pickle.load(c)
    #     for key, value in ncbi_cache.items():
    #         if value is not None:
    #             new_cache[key] = value
    # with open("network/cache/ncbi_id.pickle", "wb") as c:
    #     pickle.dump(new_cache, c)

    # # Cache to round all of the geonames points
    # new_cache = {}
    # with open("network/cache/geonames_point_cache.pickle", "rb") as c:
    #     point_cache = pickle.load(c)
    #     for key, value in point_cache.items():
    #         lat, long = map(float, key)  # Convert lat and long to floats
    #         new_key = round(lat, 2), round(long, 2)
    #         new_cache[new_key] = value
        
    # with open("network/cache/geonames_point_cache_rounded.pickle", "wb") as c:
    #     pickle.dump(new_cache, c)

    await ingest_wahis(database_handler, geonames_api, ncbi_api)
    await ingest_flunet(database_handler, geonames_api, ncbi_api)
    await ingest_gmpd(database_handler, geonames_api, ncbi_api)

    # # Keep combine and worldpop at the end of ingestion
    await ingest_combine(database_handler)
    await ingest_worldpop(database_handler, geonames_api)


if __name__ == "__main__":
    asyncio.run(main())
