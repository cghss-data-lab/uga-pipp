import flunet
import ncbi
from geonames import merge_geo, country_shp
from loguru import logger
from datetime import datetime

def search_and_merge(taxId, SESSION):
    logger.info(f'CREATING node {taxId}')
    ncbi_metadata = ncbi.get_metadata(taxId)
    taxon = {**ncbi_metadata, "taxId":taxId}
    ncbi.merge_taxon(taxon, SESSION)

# Define function to format date string into ISO format
def get_iso_date(date_str):
    date_obj = datetime.strptime(date_str, '%m/%d/%y')
    return date_obj.strftime('%Y-%m-%d')

def ingest_flunet(SESSION):
    try:
        flunet_rows = flunet.get_rows()

        # get mapping from flunet columns
        # to agents or agent groups
        columns = flunet_rows[0].keys()
        agent_groups = flunet.get_agent_groups(columns)

        # Make sure the agent groups and their
        # taxons exist in the database
        flunet.merge_agent_groups(agent_groups, SESSION)

        human = 9606 # Tax ID for humans
        # Create human node
        search_and_merge(human, SESSION)

        for index, row in enumerate(flunet_rows):
            
            # Skip rows where no data
            if row["Collected"] == "" and row["Processed"] == "" and row["Total positive"] == "" and row["Total negative"] == "":
                continue

            logger.info(f"Creating FluNet Report {index}")

            country = row["Territory"]
            merge_geo(country, SESSION)

            # eventId is disease, country, reportdate
            eventId = "Flu-" + str(country) + "-" + str(row["Start date"])
            reportId = "FluNet-" + str(index)

            # Create the report node
            create_report_query = f"""
                MATCH(g:Geography {{name: "{country}"}})
                MERGE (r:Report:FluNet {{
                    dataSource: '{"FluNet"}',
                    reportId: {reportId},
                    reportDate: date('{get_iso_date(row["Start date"])}')
                }})

            """

            SESSION.run(create_report_query)

            for col in agent_groups.keys():
                # skip detection columns with no values
                # or with zero specimens detected
                if not row[col] or row[col] == "0":
                    continue

                ncbi_id = int(agent_groups[col])
                event_rel_props = {
                    "subtype": col,
                    "role": "pathogen"
                }

                create_event_query = f"""
                    MATCH (r:Report:FluNet {{reportId: {reportId}}})
                    MERGE (r)-[:REPORTS]->(e:Event:Outbreak {{
                        eventId: "{eventId}",
                        startDate: date('{get_iso_date(row["Start date"])}'),
                        endDate: date('{get_iso_date(row["End date"])}'),
                        duration: 'P7D'
                    }})
                    WITH e
                    MATCH (g:Geography {{name: "{country}"}})
                    MERGE (e)-[:OCCURS_IN]->(g)
                    WITH e
                    MERGE (t:Taxon {{taxId: {ncbi_id}}})
                    MERGE (e)-[:INVOLVES {{
                                            subtype: '{event_rel_props['subtype']}',
                                            role: '{event_rel_props['role']}',
                                            totalSpecimensCollected: {int(row["Collected"] or 0)},
                                            totalSpecimensProcessed: {int(row["Processed"] or 0)},
                                            totalSpecimensPositive: {int(row[col])}
                    }}]->(t)
                """

                SESSION.run(create_event_query)

            # Create the INVOLVES relationships for humans
            create_human_query = f"""
                    MATCH (t:Taxon {{taxId: {human}}})
                    MATCH (e:Event:Outbreak {{eventId: "{eventId}"}})
                    MERGE (e)-[:INVOLVES {{caseCount: {int(row['Total positive'] or 0)}, role: 'host'}}]->(t)
                """

            SESSION.run(create_human_query)
        
        country_shp(SESSION)

    except Exception as e:
        logger.error(f"An exception occurred: {e}")
        raise
