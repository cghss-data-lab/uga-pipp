from geonames.search_lat_long import search_lat_long
from geonames import merge_geo
import gmpd
from loguru import logger
from datetime import datetime
from functools import cache

def ingest_gmpd(SESSION):
    gmpd_rows = gmpd.get_rows()
    
    # Make sure taxons exist in the database
    # Cast variables for properties
    for index, row in enumerate(gmpd_rows):

        reference = row["Citation"]

        # Calculated specimens positive
        totalSpecimensCollected = row["NumSamples"]
        if totalSpecimensCollected and totalSpecimensCollected != "NA":
            totalSpecimensCollected = int(totalSpecimensCollected)

            prevalence = row["Prevalence"]
            if prevalence and prevalence != "NA":
                prevalence = float(prevalence)
                totalSpecimensPositive = round(prevalence * totalSpecimensCollected)
            else:
                totalSpecimensPositive = "Unknown"

        detectionType = row["SamplingType"]    
        dataSource = "GMPD"
        long = row["Longitude"]
        lat = row ["Latitude"]  
        reportId = "GMPD-" + str(index)


        # Return the location geonameId there is one
        geonameId = search_lat_long(lat, long)

        # If there is not geonameId, just create the Report node
        if geonameId is None:
            logger.warning(f"No location found for lat: {lat}, long: {long}")
            query = """
                MERGE (r:GMPD:Report {dataSource: $dataSource, 
                                            reportId:$reportId,
                                            reference:$reference})
                RETURN r
                """

            parameters = {
                "dataSource": dataSource,
                "reportId": reportId, 
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
                                            reportId: $reportId,
                                            reference: $reference})
                MERGE (r)-[:ABOUT]->(g)
                RETURN r
            """

            parameters = {
                "geonameId": geonameId,
                "dataSource": dataSource,
                "reportId": reportId, 
                "reference": reference
            }

            SESSION.run(query, parameters)

        host_ncbi_id, pathogen_ncbi_id = gmpd.link_gmpd_to_ncbi(row, SESSION)

        if host_ncbi_id:
            host_ncbi_id = int(host_ncbi_id)
            role = "host"
            # Create the relationships between the Report node and the host taxon
            query = """
            MATCH (r:GMPD:Report {reportId: $reportId}), (h:Taxon {taxId: $host_ncbi_id})
            MERGE (r)-[hr:MENTIONS {role: $role}]->(h)
            """
            parameters = {"reportId": reportId, "host_ncbi_id": host_ncbi_id, "role":role}
            SESSION.run(query, parameters)

        if pathogen_ncbi_id:
            pathogen_ncbi_id = int(pathogen_ncbi_id)
            role = "pathogen"
            # Create the relationships between the Report node and the pathogen taxon
            query = """
            MATCH (r:GMPD:Report {reportId: $reportId}), (p:Taxon {taxId: $pathogen_ncbi_id})
            MERGE (r)-[pr:MENTIONS {role: $role, detectionType: $detectionType, totalSpecimensCollected: $totalSpecimensCollected, totalSpecimensPositive: $totalSpecimensPositive}]->(p)
            """
            parameters = {"reportId": reportId, "pathogen_ncbi_id": pathogen_ncbi_id, "role":role, "detectionType":detectionType,"totalSpecimensCollected":totalSpecimensCollected, "totalSpecimensPositive":totalSpecimensPositive}
            SESSION.run(query, parameters)

        if host_ncbi_id is not None and pathogen_ncbi_id is not None:
            # Create a symmetric relationship between taxa
            pairings_query = (
                "MATCH (t1:Taxon {taxId: $host_ncbi_id}), (t2:Taxon {taxId: $pathogen_ncbi_id}) "
                "MERGE (t2)-[:ASSOCIATED_WITH]->(t1) "
                "MERGE (t1)-[:ASSOCIATED_WITH]->(t2) "
            )

            pairings_params = {"host_ncbi_id": host_ncbi_id, "pathogen_ncbi_id":pathogen_ncbi_id}
            SESSION.run(pairings_query, pairings_params)


