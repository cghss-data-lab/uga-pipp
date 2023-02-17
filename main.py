import os
import time

from neo4j import GraphDatabase
from dotenv import load_dotenv

import gmpd


load_dotenv()
# pull env vars for auth and create neo4j driver
NEO4J_AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
SESSION = NEO4J_DRIVER.session()

def add_taxons():
    keys = gmpd.get_unique_taxons()

    # Get the hosts and label as both taxons and hosts
    for key in keys[0]:
        # print(f'CREATE (:Taxon:Host {{species: "{key}"}}')
        SESSION.run(
            f'CREATE (:Taxon:Host {{species: "{key}"}})'
        )
    
    # Get the pathogens and label as both taxons and pathogens
    for key in keys[1]:
        # print(f'CREATE (:Taxon:Pathogen {{species: "{key}"}}')
        SESSION.run(
            f'CREATE (:Taxon:Pathogen {{species: "{key}"}})'
        )
    

if __name__ == "__main__":
    gmpd_rows = gmpd.get_rows()
    add_taxons()
    # for index, row in enumerate(gmpd_rows):
         
            
        # Get the host species
        # Get the parasite species
        # Because they're on the same row, set the relationship as "Parasite infects species"
        


    

# SESSION.run('CREATE (:Message {message: "Hello World"})-[:FROM]->(:Planet {name: "Earth"})')

NEO4J_DRIVER.close()

