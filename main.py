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

if __name__ == "__main__":
    gmpd_rows = gmpd.get_rows()

    for row in gmpd_rows:

        # Iterate over dataset and create separate taxon nodes for hosts and pathogens (with labels)
        host_node = SESSION.run(
            'MERGE (n:Taxon:Host {species: $species}) RETURN n',
            species=row['species']
        ).single().get('n')
        pathogen_node = SESSION.run(
            'MERGE (n:Taxon:Pathogen {species: $ParasiteGMPD}) '
            'SET n.closeTransmission = $closeTransmission, n.noncloseTransmission = $noncloseTransmission, n.vectorTransmission = $vectorTransmission, n.intermediateTransmission = $intermediateTransmission '
            'RETURN n',
            ParasiteGMPD=row["ParasiteGMPD"],
            closeTransmission=row["close"],
            noncloseTransmission=row["nonclose"],
            vectorTransmission=row["vector"],
            intermediateTransmission = row["intermediate"]
        ).single().get("n")

        # Create relationship between hosts and pathogens
        SESSION.run(
            'MATCH (h:Taxon:Host {species: $species}), (p:Taxon:Pathogen {species: $ParasiteGMPD}) '
            'MERGE (p)-[:INFECTS]->(h)',
            species=row["species"],
            ParasiteGMPD=row["ParasiteGMPD"]
        )

    NEO4J_DRIVER.close()
