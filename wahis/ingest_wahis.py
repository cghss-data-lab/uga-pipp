from loguru import logger
from datetime import datetime

from geonames import search_lat_long
from geonames import geo_id_search
from geonames import merge_geo

import wahis

from wahis import search_and_merge

def ingest_wahis(SESSION):
    for i in range(1,5064): # as of 5/24/22
        try:
            listId = i
            evolution_list = wahis.get_evolution(listId)
            if evolution_list:
                for x in range(len(evolution_list)):
                    reportId = evolution_list[x]
                    logger.info(f'GETTING report id {reportId}')
                    metadata = wahis.get_report(reportId)

                    if 'event' not in metadata:
                        logger.error(f"Report {reportId} does not exist. Skipping...")
                        continue

                    #Sections to make it easier
                    event = metadata['event']
                    report = metadata['report']
                    outbreaks = metadata['outbreaks']

                    
                    # # Set for use with lineage
                    # previousReportId = int(report['previousReportId'])

                    # On the report node
                    reported_str = report['reportedOn']
                    reported_strip = datetime.strptime(reported_str, '%Y-%m-%dT%H:%M:%S.%f%z')
                    reported = reported_strip.strftime('%Y-%m-%d')
                    reasonForNotification = event['reason']['translation']
                    eventComment = event['eventComment']
                    eventDescription = eventComment['translation'] if eventComment and 'translation' in eventComment else 'N/A'

                    iso3 = event['country']['isoCode']

                    # MERGE a REPORT node 
                    report_params = {
                        "dataSource":"WAHIS",
                        "reportId":reportId,
                        "reportDate":reported,
                        "reasonForReport":reasonForNotification,
                        "description":eventDescription
                    }

                    report_query = """
                        MERGE (r:Report:WAHIS {dataSource: $dataSource, 
                                        reportId:$reportId,
                                        reportDate:$reportDate,
                                        reasonForReport:$reasonForReport,
                                        description:$description})
                        RETURN r
                        """

                    # RUN query to create REPORT node
                    logger.info(f' MERGE report ID: {reportId}')
                    SESSION.run(report_query, report_params)

                    # For each outbreak event, get metadata
                    for key in outbreaks:

                        #EVENT :IN GEO
                        place = key['location']
                        long = key['longitude']
                        lat = key['latitude']

                        # Return the location geonameId there is one
                        if lat and long:
                            geonameId = search_lat_long(lat, long)
                            merge_geo(geonameId, SESSION)
                        elif place:
                            geonameId = geo_id_search(place)
                            merge_geo(geonameId, SESSION)
                        else:
                            geonameId = None #TODO: Use iso3 

                        # For event/outbreak node
                        eventId = key['outbreakId']
                        outbreak_metadata = wahis.get_outbreak(reportId, eventId)

                        outbreak_str = outbreak_metadata['outbreak']['startDate']
                        start_strip = datetime.strptime(outbreak_str, '%Y-%m-%dT%H:%M:%S.%f%z')
                        outbreakStart = start_strip.strftime('%Y-%m-%d')

                        outbreak_end = outbreak_metadata['outbreak']['endDate']
                        end_strip = datetime.strptime(outbreak_end, '%Y-%m-%dT%H:%M:%S.%f%z')
                        outbreakEnd = end_strip.strftime('%Y-%m-%d')

                        # Connect to NCBI using serotype if it's available
                        # Otherwise use pathogen name
                        subtype = event['subType']
                        if subtype and 'name' in subtype:
                            serotype = event['subType']['name']
                            pathogen_ncbi = wahis.search_and_merge(serotype, SESSION)
                        else:
                            pathogen = event['causalAgent']['name']
                            pathogen_ncbi = wahis.search_and_merge(pathogen, SESSION)

                        if pathogen_ncbi:
                            pathogen_ncbi_id = int(pathogen_ncbi)

                            # MERGE an EVENT node with the Geo node and pathogen Taxon
                            event_query = f"""
                                MATCH (r:Report:WAHIS {{reportId: $reportId}})
                                MERGE (r)-[:REPORTS]->(e:Event:Outbreak {{
                                    eventID: $eventId,
                                    startDate: $outbreakStart,
                                    endDate: $outbreakEnd
                                }})
                                WITH e
                                MATCH (g:Geography {{geonameId: $geonameId}})
                                MERGE (e)-[:IN]->(g)
                                WITH e
                                MERGE (t:Taxon {{taxId: $pathogen_ncbi_id}})
                                MERGE (e)-[:INVOLVES {{
                                    role: "pathogen"
                                }}]->(t)
                            """

                            event_params = {
                                "reportId": reportId,
                                "eventId": int(eventId),
                                "outbreakStart": outbreakStart,
                                "outbreakEnd": outbreakEnd,
                                "geonameId": geonameId,
                                "pathogen_ncbi_id": pathogen_ncbi_id
                            }

                            logger.info(f' MERGE event ID: ({eventId})')
                            SESSION.run(event_query, event_params)

                            
                        # Create the event / host Taxon relationship
                        species_quantities = outbreak_metadata['speciesQuantities']                    
                        if species_quantities:
                            for key in species_quantities:
                                newQuants = key['newQuantities']
                                if newQuants:
                                    speciesName = key['newQuantities']['speciesName']
                                    speciesWild = key['newQuantities']['isWild']
                                    caseCount = key['newQuantities']['cases'] if 'cases' in key['newQuantities'] else "N/A"
                                    deathCount = key['newQuantities']['deaths'] if 'deaths' in key['newQuantities'] else "N/A"
                        else:
                            quantData = report['quantitativeData']
                            if quantData:
                                newQuantData = quantData['news']
                                if newQuantData:
                                    for key in newQuantData:
                                        speciesName = key['speciesName']
                                        speciesWild = key['isWild']
                                        caseCount = key['cases'] if 'cases' in newQuantData else "N/A"
                                        deathCount = key['deaths'] if 'deaths' in newQuantData else "N/A"
                        
                        # Check if caseCount and deathCount are available
                        if caseCount != "N/A":
                            caseCount = int(caseCount)
                        if deathCount != "N/A":
                            deathCount = int(deathCount)

                        host_ncbi = wahis.search_and_merge(speciesName, SESSION)
                        if host_ncbi:
                            host_ncbi_id = int(host_ncbi)
                    
                            host_event_rel = """
                                MATCH (t:Taxon {taxId: $host_ncbi_id})
                                MATCH (e:Event:Outbreak {eventId: $eventId})
                                MERGE (e)-[:INVOLVES {
                                    caseCount: $caseCount,
                                    deathCount: $deathCount,
                                    role: 'host',
                                    speciesWild: $speciesWild
                                }]->(t)
                            """

                            host_params = {
                                "host_ncbi_id": host_ncbi_id,
                                "eventId": int(eventId),
                                "caseCount": caseCount,
                                "deathCount": deathCount,
                                "speciesWild": speciesWild
                            }

                            logger.info(f' MERGE host: ({speciesName})')
                            SESSION.run(host_event_rel, host_params)

                    # # Create relationship to parent, except for first report 
                    # if i > 0:
                    #     parent = evolution_list[i-1]
                    #     if parent == previousReportId:
                    #         SESSION.run("""
                    #             MATCH (child:Report {reportId: $reportId})
                    #             MATCH (parent:Report {reportId: $reportId})
                    #             MERGE (child)-[:FOLLOWS]->(parent)
                    #         """, {"reportId": reportId})

        except Exception as e:
            logger.error(f'An exception occurred: {e}')
            raise




                    


        