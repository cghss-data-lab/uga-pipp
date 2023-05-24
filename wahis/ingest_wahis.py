from loguru import logger
from datetime import datetime

from geonames import search_lat_long
from geonames import geo_id_search
from geonames import merge_geo
from wahis import get_evolution
from wahis import get_outbreak
from wahis import get_report
from wahis import search_and_merge

def ingest_wahis(SESSION):
    for i in range(1,5064): # as of 5/24/22
        listId = i
        evolution_list = get_evolution(listId)
        if evolution_list:
            for x in range(len(evolution_list)):
                reportId = evolution_list[x]
                logger.info(f'GETTING report id {reportId}')
                metadata = get_report(reportId)

                #Sections to make it easier
                event = metadata['event']
                report = metadata['report']
                outbreaks = metadata['outbreaks']
                
                # Set for use with lineage
                previousReportId = int(report['previousReportId'])

                # On the report node
                reported = datetime(report['reportedOn']).strftime('%Y-%m-%d')
                reasonForNotification = event['reason']['translation']
                eventDescription = event['eventComment']['translation']

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
                logger.info(f' MERGE report ID: ({reportId})')
                SESSION.run(report_query, report_params)

                # For each outbreak event, get metadata
                for key in outbreaks:

                    #EVENT :IN GEO
                    place = outbreaks[key]['location']
                    long = outbreaks[key]['longitude']
                    lat = outbreaks[key]['latitude']

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
                    eventId = outbreaks[key]['outbreakId']
                    outbreak_metadata = get_outbreak(reportId, eventId)
                    outbreakStart = outbreak_metadata['outbreak']['startDate']
                    outbreakEnd = outbreak_metadata['outbreak']['endDate']

                    # Connect to NCBI using serotype if it's available
                    # Otherwise use pathogen name
                    serotype = event['subType']['name']
                    pathogen = event['causalAgent']['name']
                    pathogen_ncbi_id = int(search_and_merge(serotype, SESSION))
                    if not pathogen_ncbi_id:
                        pathogen_ncbi_id = int(search_and_merge(pathogen, SESSION))

                    # MERGE an EVENT node with the Geo node and pathogen Taxon
                    event_query = f"""
                        MATCH (r:REPORT:WAHIS {{{reportId}}})
                        MERGE (r)-[:REPORTS]->(e:Event:Outbreak {{
                            eventID: {eventId},
                            startDate: {outbreakStart},
                            endDate: {outbreakEnd}
                        }})
                        WITH e
                        MATCH (g:Geography {{geonameId: {geonameId}}})
                        MERGE (e)-[:IN]->(g)
                        WITH e
                        MERGE (t:Taxon {{taxId: {pathogen_ncbi_id}}})
                        MERGE (e)-[:INVOLVES {{
                            role: "pathogen"
                        }}]->(t)
                    """

                    logger.info(f' MERGE event ID: ({eventId})')
                    SESSION.run(event_query)
                        
                    # Create the event / host Taxon relationship
                    species_quantities = outbreak_metadata['speciesQuantities']                    
                    for key in species_quantities:
                        speciesName = species_quantities[key]['newQuantities']['speciesName']
                        speciesWild = species_quantities[key]['newQuantities']['isWild']
                        caseCount = species_quantities[key]['newQuantities']['cases']
                        deathCount = species_quantities[key]['newQuantities']['deaths']

                        host_ncbi_id = int(search_and_merge(speciesName, SESSION))

                        host_event_rel = f"""
                            MATCH (t:Taxon {{taxId: {host_ncbi_id}}})
                            MATCH (e:Event:Outbreak {{eventId: "{eventId}"}})
                            MERGE (e)-[:INVOLVES {{
                                caseCount: {int({caseCount}) or "NA"},
                                deathCount: {int({deathCount}) or "NA"}, 
                                role: 'host',
                                speciesWild: {speciesWild}
                            }}]->(t)
                        """

                        SESSION.run(host_event_rel)

                # # Create relationship to parent, except for first report 
                # if i > 0:
                #     parent = evolution_list[i-1]
                #     if parent == previousReportId:
                #         SESSION.run("""
                #             MATCH (child:Report {reportId: $reportId})
                #             MATCH (parent:Report {reportId: $reportId})
                #             MERGE (child)-[:FOLLOWS]->(parent)
                #         """, {"reportId": reportId})



                


    