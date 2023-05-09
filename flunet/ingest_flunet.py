import flunet
import ncbi
from geonames import merge_geo
from loguru import logger
from datetime import datetime

def search_and_merge(TaxId, SESSION):
    logger.info(f'CREATING node {TaxId}')
    ncbi_metadata = ncbi.get_metadata(TaxId)
    taxon = {**ncbi_metadata, "TaxId":TaxId}
    ncbi.merge_taxon(taxon, SESSION)

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
        search_and_merge(human, SESSION)

        for index, row in enumerate(flunet_rows):
            # skip rows where no samples were collected
            if row["Collected"] == "" or row["Collected"] == "0":
                continue

            logger.info(f"Creating FluNet Report {index}")

            country = row["Territory"]
            merge_geo(country,SESSION)

            # Create the report node
            # Parse the date string into a datetime object
            start_date_str = row["Start date"]
            start_date_obj = datetime.strptime(start_date_str, "%m/%d/%y")

            # Parse the date string into a datetime object
            end_date_str = row["End date"]
            end_date_obj = datetime.strptime(end_date_str, "%m/%d/%y")

            report_props = {
                "dataSource":"FluNet",
                "dataSourceRow":index,
                "reportDate":start_date_obj,
            }

            create_report_query = f"""
                MATCH (g:Geography {{name: '{country}'}})
                MERGE (report:Report:FluNet {{ {', '.join([f"{k}: {v}" for k, v in report_props.items()])} }})
                CREATE (report)-[:IN]->(g)
            """

            create_event_queries = []
            for col in agent_groups.keys():
                # skip detection columns with no values
                # or with zero specimens detected
                if not row[col] or row[col] == "0":
                    continue

                ncbi_id = int(agent_groups[col])
                event_relationship_props = {
                    "subtype": col,
                    "role": "pathogen",
                    "detectionCount":int(row[col])
                }

                event_props = {
                    "startDate":start_date_obj,
                    "endDate":end_date_obj,
                    "duration":{"days":7},
                    "totalSpecimensCollected":int(row["Collected"]),
                    "totalSpecimensProcessed":int(row["Processed"] or 0),
                    "totalSpecimensPositive":int(row["Total positive" or 0]),
                    "totalSpecimensNegative":int(row["Total negative"] or 0),
                }

                create_event_query = f"""
                    MATCH (taxon:Taxon {{TaxId: {ncbi_id}}})
                    MERGE (event:Event:Outbreak {{ {', '.join([f"{k}: {v}" for k, v in event_props.items()])} }})                    
                    CREATE (report)-[:REPORTS]->(event)
                    CREATE (event)-[:INVOLVES {{{', '.join([f"{k}: {v}" for k, v in event_relationship_props.items()])}}}]->(taxon)
                """

                create_event_queries.append(create_event_query)

            # Create the INVOLVES relationships for humans
            create_human_query = f"""
                MATCH (taxon:Taxon {{TaxId: {human}}})
                CREATE (event)-[:INVOLVES {{caseCount: {int(row.get('Total positive', 0))}, role: 'host'}}]->(taxon)
            """

            # Write the full Cypher query
            cypher_query = create_report_query + "\n" + "\n".join(create_event_queries) + "\n" + create_human_query

            # Execute the Cypher query
            SESSION.run(cypher_query)

    except Exception as e:
        logger.error(f"An exception occurred: {e}")
        raise
