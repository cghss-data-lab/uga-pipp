from geonames.search_lat_lng import search_lat_lng
from geonames import merge_geo
import gmpd
from loguru import logger
from datetime import datetime
from functools import cache

def ingest_gmpd(SESSION):
    gmpd_rows = gmpd.get_rows()
    
    # Make sure taxons exist in the database
    # Cast variables for properties
    for dataSourceRow, row in enumerate(gmpd_rows):

        reference = row["Citation"]
        totalSpecimensCollected = row["HostsSampled"]
        detectionType = row["SamplingType"]    
        dataSource = "GMPD"
        lng = row["Longitude"]
        lat = row ["Latitude"]  

        # Return the location geonameId there is one
        geonameId = search_lat_lng(lat, lng)

        # If there is not geonameId, just create the Report node
        if geonameId is None:
            logger.warning(f"No location found for lat: {lat}, lng: {lng}")
            query = """
                MERGE (r:GMPD:Report {dataSource: $dataSource, 
                                            dataSourceRow:$dataSourceRow,
                                            reference:$reference})
                RETURN r
                """

            parameters = {
                "dataSource": dataSource,
                "dataSourceRow": dataSourceRow, 
                "reference": reference
            }
            result = SESSION.run(query, parameters)

        # If there IS a geonameId, make sure it's an integer, create the Geo and Report nodes
        else:
            geonameId = int(geonameId)
            merge_geo(geonameId, SESSION)

            # Create the Report node if it doesn't exist, and set its label to GMPD
            query = """
                MATCH (g:Geography {geonameId: $geonameId})
                MERGE (r:GMPD:Report {dataSource: $dataSource, 
                                            dataSourceRow: $dataSourceRow,
                                            reference: $reference})
                MERGE (r)-[:IN]->(g)
                RETURN r
            """

            parameters = {
                "geonameId": geonameId,
                "dataSource": dataSource,
                "dataSourceRow": dataSourceRow, 
                "reference": reference
            }

            SESSION.run(query, parameters)

        host_ncbi_id, pathogen_ncbi_id = gmpd.link_gmpd_to_ncbi(row, SESSION)

        if host_ncbi_id:
            host_ncbi_id = int(host_ncbi_id)
            role = "host"
            # Create the relationships between the Report node and the host taxon
            query = """
            MATCH (r:GMPD:Report {dataSourceRow: $dataSourceRow}), (h:Taxon {TaxId: $host_ncbi_id})
            MERGE (r)-[hr:ASSOCIATES {role: $role, detectionType: $detectionType, totalSpecimensCollected: $totalSpecimensCollected}]->(h)
            """
            parameters = {"dataSourceRow": dataSourceRow, "host_ncbi_id": host_ncbi_id, "role":role, "detectionType":detectionType, "totalSpecimensCollected":totalSpecimensCollected}
            SESSION.run(query, parameters)

        if pathogen_ncbi_id:
            pathogen_ncbi_id = int(pathogen_ncbi_id)
            role = "pathogen"
            # Create the relationships between the Report node and the pathogen taxon
            query = """
            MATCH (r:GMPD:Report {dataSourceRow: $dataSourceRow}), (p:Taxon {TaxId: $pathogen_ncbi_id})
            MERGE (r)-[pr:ASSOCIATES {role: $role, detectionType: $detectionType, totalSpecimensCollected: $totalSpecimensCollected}]->(p)
            """
            parameters = {"dataSourceRow": dataSourceRow, "pathogen_ncbi_id": pathogen_ncbi_id, "role":role, "detectionType":detectionType,"totalSpecimensCollected":totalSpecimensCollected}
            SESSION.run(query, parameters)


