import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
# pull env vars for auth and create neo4j driver
NEO4J_AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
SESSION = NEO4J_DRIVER.session()

SESSION.run('CREATE (:Message {message: "Hello World"})-[:FROM]->(:Planet {name: "Earth"})')

NEO4J_DRIVER.close()

