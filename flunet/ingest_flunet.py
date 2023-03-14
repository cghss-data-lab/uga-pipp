import flunet
from loguru import logger

def ingest_flunet():
    flunet_rows = flunet.get_rows()

    # get mapping from flunet columns
    # to agents or agent groups
    columns = flunet_rows[0].keys()
    agent_groups = flunet.get_agent_groups(columns)
    
    # Make sure the agent groups and their taxons exist in the database
    flunet.merge_agent_groups(agent_groups, SESSION)

    created_countries = set()
    created_transmission_zones = set()

    for index, row in enumerate(flunet_rows):
        # skip rows where no samples were collected
        if not row.get("Collected") or row["Collected"] == "0":
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

        for col, ncbi_id in agent_groups.items():
            # skip detection columns with no values or with zero specimens detected
            if not row.get(col) or row[col] == "0":
                continue

            match_agent_groups += f'\nMATCH (taxon{ncbi_id}:Taxon {{TaxId: $ncbi_id_{ncbi_id}}}) '
            create_group_relationships += f"CREATE (report)-[:IDENTIFIES {{count: $row_{col}}}]->(taxon{ncbi_id}) "

        params = {}
        for col in columns:
            params[f"row_{col}"] = row.get(col) or 0
        for col, ncbi_id in agent_groups.items():
            params[f"ncbi_id_{ncbi_id}"] = ncbi_id
        params["country"] = country

        query = (
            "MATCH (c:Country {name: $country})"
            + match_agent_groups
            + "\nCREATE (report:FluNet:Report {"
            "  flunetRow: $index, "
            '  start: date($row_Start_date), '
            "  duration: duration({days: 7}), "
            '  collected: $row_Collected, '
            '  processed: $row_Processed, '
            '  positive: $row_Total_positive, '
            '  negative: $row_Total_negative '
            "})-[:IN]->(c)"
            + create_group_relationships
        )

        try:
            SESSION.run(query, params)
        except Exception as e:
            logger.error(f"Failed to create FluNet report {index}: {str(e)}")
