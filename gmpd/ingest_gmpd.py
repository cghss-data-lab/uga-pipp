import gmpd
from loguru import logger
from datetime import datetime

def ingest_gmpd(SESSION):
    gmpd_rows = gmpd.get_rows()

    # Make sure taxons exist in the database
    # Cast variables for properties
    for row in gmpd_rows:
        host_tax_id, pathogen_tax_id = gmpd.link_gmpd_to_ncbi(row, SESSION)
        citation = row["Citation"]
        prevalence = row["Prevalence"]
        collected = row["Hosts Sampled"]
        
        # Create the Report node if it doesn't exist, and set its label to GMPD
        query = """
        MERGE (r:GMPD:Report {citation:$citation, prevalence:$prevalence, collected:$collected})
        RETURN r
        """
        parameters = {"citation": citation, "prevalence": prevalence, "collected": collected}
        result = SESSION.run(query, parameters)
        report_node = result.single()[0]

        # Create the relationships between the Report node and the host / pathogen taxons
        query = """
        MATCH (r:GMPD:Report), (h:Taxon {TaxId: $host_tax_id}), (p:Taxon {TaxId: $pathogen_tax_id})
        MERGE (r)-[hr:REPORTS {host: 1}]->(h)
        MERGE (r)-[pr:REPORTS {pathogen: 1}]->(p)
        """
        parameters = {"host_tax_id": host_tax_id, "pathogen_tax_id": pathogen_tax_id}
        result = SESSION.run(query, parameters)



    # for index, row in enumerate(gmpd_rows):
    #     logger.info(f"Creating GMPD record {index}")

        ## if it's a new location, create before continuing
        # if loc not in locations:
            # gmpd.create_location(loc, SESSION)
            # created_location.add(loc)
        # handle row["Location"] 
        # handle row["Latitude"]
        # handle row ["Longitude"]

        # match_agent_groups = ""
        # create_group_relationships = ""


        #     ncbi_id = agent_groups[col]

        #     match_agent_groups += (
        #         f'\nMATCH (taxon{ncbi_id}:Taxon {{TaxId: "{ncbi_id}"}}) '
        #     )
        #     create_group_relationships += (
        #         f"CREATE (report)-[:REPORTS {{count: {row[col]}}}]->(taxon{ncbi_id}) "
        #     )

        #     # Parse the date string into a datetime object
        #     start_date_str = row["Start date"]
        #     start_date_obj = datetime.strptime(start_date_str, "%m/%d/%y")

            # Use the datetime object in your Cypher query
            # SESSION.run(
            #     f'MATCH (c:Country {{name: "{country}"}}) '
            #     + match_agent_groups
            #     + f"\nMERGE (report:FluNet:Report {{"
            #     f"  flunetRow: {index}, "
            #     f'  start: date("{start_date_obj.date()}"), '
            #     f"  duration: duration({{days: 7}}), "
            #     f'  collected: {row["Collected"] or 0}, '
            #     f'  processed: {row["Processed"] or 0}, '
            #     f'  positive: {row["Total positive"] or 0}, '
            #     f'  negative: {row["Total negative"] or 0} '
            #     f"}})-[:IN]->(c)" + create_group_relationships)
