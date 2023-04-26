import flunet
from geonames import merge_geo
from loguru import logger
from datetime import datetime

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

        for index, row in enumerate(flunet_rows):
            # skip rows where no samples were collected
            if row["Collected"] == "" or row["Collected"] == "0":
                continue

            logger.info(f"Creating FluNet Report {index}")

            country = row["Territory"]

            merge_geo(country,SESSION)

            match_agent_groups = ""
            create_group_relationships = ""

            for col in agent_groups.keys():
                # skip detection columns with no values
                # or with zero specimens detected
                if not row[col] or row[col] == "0":
                    continue

                ncbi_id = int(agent_groups[col])

                match_agent_groups += (
                    f'\nMATCH (taxon{ncbi_id}:Taxon {{TaxId: {ncbi_id}}}) '
                )
                create_group_relationships += (
                    f"CREATE (report)-[:REPORTS {{count: {row[col]}, subtype:'{col}', pathogen:1}}]->(taxon{ncbi_id}) ")

                # Parse the date string into a datetime object
                start_date_str = row["Start date"]
                start_date_obj = datetime.strptime(start_date_str, "%m/%d/%y")

            # Write query with metadata
            cypher_query = (
                f'MATCH (g:Geography {{name: "{country}"}}) '
                + match_agent_groups 
                + f"\nMERGE (report:FluNet:CaseReport {{"
                f"  dataSource: 'FluNet', "
                f"  dataSourceRow: {index}, "
                f'  start: date("{start_date_obj.date()}"), '
                f"  duration: duration({{days: 7}}), "
                f'  collected: {row["Collected"] or 0}, '
                f'  processed: {row["Processed"] or 0}, '
                f'  positive: {row["Total positive"] or 0}, '
                f'  negative: {row["Total negative"] or 0} '
                f"}})-[:IN]->(g)" + create_group_relationships
            )

            # Execute the Cypher query
            SESSION.run(cypher_query)

    except Exception as e:
        logger.error(f"An exception occurred: {e}")
        raise
