import os

from neo4j import GraphDatabase
from dotenv import load_dotenv

import carnivoreGMPD
import link_ncbi_gmpd

load_dotenv()

# Pull env vars for auth and create neo4j driver
NEO4J_AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
SESSION = NEO4J_DRIVER.session()

if __name__ == "__main__":
    tax_list = set()
    gmpd_rows = carnivoreGMPD.get_rows()

    for row in gmpd_rows:

        host_species = row['host_species']
        pathogen_species = row['pathogen_species']

        # Check if host or pathogen has been processed yet
        if host_species in tax_list:
            print(f"{host_species} has already been checked")
            continue
        elif pathogen_species in tax_list:
            print(f"{pathogen_species} has already been checked")
            continue
        else:
            # Add the species to the set
            tax_list.add(host_species)
            tax_list.add(pathogen_species)

        # Create taxa from NCBI
        link_ncbi_gmpd.create_ncbi_taxon(host_species, pathogen_species, SESSION)

        # Label the NCBI taxon nodes
        link_ncbi_gmpd.label_ncbi_taxon(host_species, pathogen_species, SESSION)

        # Create pairings
        link_ncbi_gmpd.link_host_pathogen(host_species, pathogen_species, SESSION)

    NEO4J_DRIVER.close()
