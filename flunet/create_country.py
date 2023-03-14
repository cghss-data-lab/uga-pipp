import logging

logger = logging.getLogger(__name__)

def create_country(country_name, zone_name, session):
    """
    For now, just merges the country into its zone,
    but this should be expanded to use geonames.
    """
    logger.info(f"Creating country node ({country_name})")

    query = """
        MATCH (zone:TransmissionZone {name: $zone_name})
        CREATE (n:Country:Geo {name: $country_name})-[:IN]->(zone)
    """

    parameters = {"country_name": country_name, "zone_name": zone_name}

    try:
        result = session.run(query, parameters)
        logger.debug(f"Neo4j query result: {result.summary().counters}")
        
    except Exception as e:
        logger.exception(f"Error running Neo4j query: {e}")
