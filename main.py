import os

from neo4j import GraphDatabase
from dotenv import load_dotenv

from carnivoreGMPD import ingest_carnivoreGMPD
from flunet import ingest_flunet

load_dotenv()

# Pull env vars for auth and create neo4j driver
NEO4J_AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
SESSION = NEO4J_DRIVER.session()

def get_geonames_id(name):
    pass

if __name__ == "__main__":
    ingest_carnivoreGMPD(SESSION)
    # ingest_flunet(SESSION)

    NEO4J_DRIVER.close()
