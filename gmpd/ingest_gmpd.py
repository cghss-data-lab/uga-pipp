import gmpd
from loguru import logger
from datetime import datetime

def ingest_gmpd(SESSION):
    gmpd_rows = gmpd.get_rows()

    # Make sure taxons exist in the database
    # Cast variables for properties
    for dataSourceRow, row in enumerate(gmpd_rows):

        citation = row["Citation"]
        prevalence = float(row["Prevalence"])
        collected = int(row["HostsSampled"])
        sampleType = row["SamplingType"]    
        dataSource = "GMPD"    

        # Create the Report node if it doesn't exist, and set its label to GMPD
        query = """
        MERGE (r:GMPD:Report {dataSource: $dataSource, 
                                dataSourceRow:$dataSourceRow,
                                citation:$citation, 
                                sampleType:$sampleType, 
                                start: $start,
                                duration: $duration, 
                                collected: $collected, 
                                processed: $processed, 
                                positive: $positive, 
                                negative: $negative, 
                                prevalence:$prevalence})
        RETURN r
        """

        parameters = {
            "dataSource": dataSource,
            "dataSourceRow": dataSourceRow, 
            "citation": citation, 
            "sampleType": sampleType,
            "start": None, 
            "duration": None,
            "collected": collected,
            "processed": processed,
            "positive": None,
            "negative": None,
            "prevalence": prevalence, 
        }

        result = SESSION.run(query, parameters)
        report_node = result.single()[0]

        host_ncbi_id, pathogen_ncbi_id = gmpd.link_gmpd_to_ncbi(row, SESSION)

        if host_ncbi_id:

            # Create the relationships between the Report node and the host taxon
            query = """
            MATCH (r:GMPD:Report {dataSourceRow: $dataSourceRow}), (h:Taxon {TaxId: $host_ncbi_id})
            MERGE (r)-[hr:REPORTS {host: 1}]->(h)
            """
            parameters = {"dataSourceRow": dataSourceRow,"host_ncbi_id": host_ncbi_id}
            result = SESSION.run(query, parameters)

        if pathogen_ncbi_id:

            # Create the relationships between the Report node and the pathogen taxon
            query = """
            MATCH (r:GMPD:Report {dataSourceRow: $dataSourceRow}), (p:Taxon {TaxId: $pathogen_ncbi_id})
            MERGE (r)-[pr:REPORTS {pathogen: 1}]->(p)
            """
            parameters = {"dataSourceRow": dataSourceRow, "pathogen_ncbi_id": pathogen_ncbi_id}
            result = SESSION.run(query, parameters)

