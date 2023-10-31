import os
from dotenv import load_dotenv
from loguru import logger
from neo4j import AsyncGraphDatabase

load_dotenv()

NEO4J_URI = os.environ["NEO4J_URI"]
NEO4J_USER = os.environ["NEO4J_USER"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
NEO4J_DATABASE = os.environ["NEO4J_DATABASE"]


class Neo4jHandler:
    def __init__(
        self,
        uri: str = NEO4J_URI,
        user: str = NEO4J_USER,
        password: str = NEO4J_PASSWORD,
        database: str = NEO4J_DATABASE,
    ) -> None:
        self.driver = AsyncGraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    async def execute_query(self, query_file: str, properties: list) -> None:
        with open(query_file, "r", encoding="utf-8") as file:
            query = file.read()
            async with self.driver.session(database=self.database) as session:
                logger.info("Ingesting into neo4j")
                await session.run(query, Mapping=properties)

    async def build_geohierarchy(
        self,
        hierarchy_list: list,
        hierarchy_query: str = "network/build_geonames_hierarchy.cypher",
    ) -> None:
        with open(hierarchy_query, "r", encoding="utf-8") as file:
            query = file.read()
            async with self.driver.session(database=self.database) as session:
                logger.info("Ingesting geonames hierarchy")
                await session.run(query, Mapping=hierarchy_list)

    async def build_ncbi_hierarchy(
        self,
        hierarchy_list: list,
        hierarchy_query: str = "network/build_ncbi_hierarchy.cypher",
    ) -> None:
        with open(hierarchy_query, "r", encoding="utf-8") as file:
            query = file.read()
            async with self.driver.session(database=self.database) as session:
                logger.info("Ingesting ncbi hierarchy")
                await session.run(query, Mapping=hierarchy_list)
