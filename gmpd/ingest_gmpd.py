from geonames.search_lat_lng import search_lat_lng
import gmpd
from loguru import logger
from datetime import datetime

def ingest_gmpd(SESSION):
    gmpd_rows = gmpd.get_rows()

    created_locs = set()
    # Make sure taxons exist in the database
    # Cast variables for properties
    for dataSourceRow, row in enumerate(gmpd_rows):

        reference = row["Citation"]
        prevalence = row["Prevalence"]
        collected = row["HostsSampled"]
        detectionType = row["SamplingType"]    
        dataSource = "GMPD"
        lng = row["Longitude"]
        lat = row ["Latitude"]  

        # Return the location geoId there is one
        geoId = search_lat_lng(lat, lng)

        if geoId is None:
            logger.warning(f"No location found for lat: {lat}, lng: {lng}")
            query = """
                MERGE (r:GMPD:CaseReport {dataSource: $dataSource, 
                                            dataSourceRow:$dataSourceRow,
                                            reference:$reference, 
                                            detectionType:$detectionType, 
                                            collected: $collected,  
                                            prevalence:$prevalence})
                RETURN r
                """

            parameters = {
                "dataSource": dataSource,
                "dataSourceRow": dataSourceRow, 
                "reference": reference, 
                "detectionType": detectionType,
                "collected": collected,
                "prevalence": prevalence
            }
            result = SESSION.run(query, parameters)

        else:
            geoId = int(geoId)
            if geoId not in created_locs:
                gmpd.create_loc(geoId, SESSION)
                created_locs.add(geoId)

                # Create the Report node if it doesn't exist, and set its label to GMPD
                query = """
                    MATCH (g:Geo {geoId: $geoId})
                    MERGE (r:GMPD:CaseReport {dataSource: $dataSource, 
                                                dataSourceRow: $dataSourceRow,
                                                reference: $reference, 
                                                detectionType: $detectionType, 
                                                collected: $collected,  
                                                prevalence: $prevalence})
                    MERGE (r)-[:IN]->(g)
                    RETURN r
                """

                parameters = {
                    "geoId": geoId,
                    "dataSource": dataSource,
                    "dataSourceRow": dataSourceRow, 
                    "reference": reference, 
                    "detectionType": detectionType,
                    "collected": collected,
                    "prevalence": prevalence
                }

                result = SESSION.run(query, parameters)

        host_ncbi_id, pathogen_ncbi_id = gmpd.link_gmpd_to_ncbi(row, SESSION)

        if host_ncbi_id:
            # Create the relationships between the Report node and the host taxon
            query = """
            MATCH (r:GMPD:CaseReport {dataSourceRow: $dataSourceRow}), (h:Taxon {TaxId: $host_ncbi_id})
            MERGE (r)-[hr:REPORTS {host: 1}]->(h)
            """
            parameters = {"dataSourceRow": dataSourceRow, "host_ncbi_id": host_ncbi_id}
            result = SESSION.run(query, parameters)

        if pathogen_ncbi_id:
            # Create the relationships between the Report node and the pathogen taxon
            query = """
            MATCH (r:GMPD:CaseReport {dataSourceRow: $dataSourceRow}), (p:Taxon {TaxId: $pathogen_ncbi_id})
            MERGE (r)-[pr:REPORTS {pathogen: 1}]->(p)
            """
            parameters = {"dataSourceRow": dataSourceRow, "pathogen_ncbi_id": pathogen_ncbi_id}
            result = SESSION.run(query, parameters)


