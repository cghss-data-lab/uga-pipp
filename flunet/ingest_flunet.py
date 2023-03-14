import flunet
from loguru import logger
from datetime import datetime

def ingest_flunet(SESSION):
    flunet_rows = flunet.get_rows()

    # get mapping from flunet columns
    # to agents or agent groups
    columns = flunet_rows[0].keys()
    agent_groups = flunet.get_agent_groups(columns)

    # Make sure the agent groups and their
    # taxons exist in the database
    flunet.merge_agent_groups(agent_groups, SESSION)

    created_countries = set()
    created_transmission_zones = set()

    for index, row in enumerate(flunet_rows):
        # skip rows where no samples were collected
        if row["Collected"] == "" or row["Collected"] == "0":
            continue

        logger.info(f"Creating FluNet Report {index}")

        zone = row["Transmission zone"]
        country = row["Territory"]

        # if it's a new zone, create it before continuing
        if zone not in created_transmission_zones:
            flunet.create_transmission_zone(zone, SESSION)
            created_transmission_zones.add(zone)

        # if it's a new country, create the country
        if country not in created_countries:
            flunet.create_country(country, zone, SESSION)
            created_countries.add(country)

        match_agent_groups = ""
        create_group_relationships = ""

        for col in agent_groups.keys():
            # skip detection columns with no values
            # or with zero specimens detected
            if not row[col] or row[col] == "0":
                continue

            ncbi_id = agent_groups[col]

            match_agent_groups += (
                f'\nMATCH (taxon{ncbi_id}:Taxon {{TaxId: "{ncbi_id}"}}) '
            )
            create_group_relationships += (
                f"CREATE (detection)-[:REPORTS {{count: {row[col]}}}]->(taxon{ncbi_id}) "
            )

            # Parse the date string into a datetime object
            start_date_str = row["Start date"]
            start_date_obj = datetime.strptime(start_date_str, "%m/%d/%y")

            # Use the datetime object in your Cypher query
            SESSION.run(
                f'MATCH (c:Country {{name: "{country}"}}) '
                + match_agent_groups
                + f"\nMERGE (detection:FluNet:Detection {{"
                f"  flunetRow: {index}, "
                f'  start: date("{start_date_obj.date()}"), '
                f"  duration: duration({{days: 7}}), "
                f'  collected: {row["Collected"] or 0}, '
                f'  processed: {row["Processed"] or 0}, '
                f'  positive: {row["Total positive"] or 0}, '
                f'  negative: {row["Total negative"] or 0} '
                f"}})-[:IN]->(c)" + create_group_relationships)
