import gmpd
from loguru import logger
from datetime import datetime

def ingest_gmpd(SESSION):
    gmpd_rows = gmpd.get_rows()

    # Make sure taxons exist in the database
    # Cast variables for properties
    for row in gmpd_rows:

        citation = row["Citation"]
        prevalence = row["Prevalence"]
        collected = row["HostsSampled"]
        sampleType = row["SamplingType"]

        # Create the Report node if it doesn't exist, and set its label to GMPD
        query = """
        MERGE (r:GMPD:Report {citation:$citation, prevalence:$prevalence, collected:$collected, sampleType:$sampleType})
        RETURN r
        """
        parameters = {"citation": citation, "prevalence": prevalence, "collected": collected, "sampleType": sampleType}
        result = SESSION.run(query, parameters)
        report_node = result.single()[0]

        host_tax_id_returned = gmpd.link_gmpd_to_ncbi(row, SESSION)
        pathogen_tax_id_returned = gmpd.link_gmpd_to_ncbi(row, SESSION)


        if host_tax_id_returned:
            host_tax_id = host_tax_id_returned

            # Create the relationships between the Report node and the host / pathogen taxons
            query = """
            MATCH (r:GMPD:Report), (h:Taxon {TaxId: $host_tax_id})
            MERGE (r)-[hr:REPORTS {host: 1}]->(h)
            """
            parameters = {"host_tax_id": host_tax_id}
            result = SESSION.run(query, parameters)

        if pathogen_tax_id_returned:
            pathogen_tax_id = pathogen_tax_id_returned

            # Create the relationships between the Report node and the host / pathogen taxons
            query = """
            MATCH (r:GMPD:Report), (p:Taxon {TaxId: $pathogen_tax_id})
            MERGE (r)-[pr:REPORTS {pathogen: 1}]->(p)
            """
            parameters = {"pathogen_tax_id": pathogen_tax_id}
            result = SESSION.run(query, parameters)

