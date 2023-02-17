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

def db_merge_gmpd_ncbi():
    keys = gmpd.get_unique_pairings()

    for key in keys:
        Host, Pathogen = key.split("|")
        ncbi_id = ncbi.id_search(f"{Host} {Pathogen}")

        if ncbi_id:
            ncbi_metadata = ncbi.get_metadata(ncbi_id)
            taxon = {**ncbi_metadata, "TaxId": ncbi_id}

            # add taxon and lineage to database
            ncbi.merge_taxon(taxon, SESSION)

        else:
            ## save broken search terms to file
            with open("not_found.txt", "a") as f:
                f.write(f"{Host}, {Pathogen}")
                f.write("\n")
                f.close()

        # resepect api rate limit
        time.sleep(0.4)

# SESSION.run('CREATE (:Message {message: "Hello World"})-[:FROM]->(:Planet {name: "Earth"})')

NEO4J_DRIVER.close()

