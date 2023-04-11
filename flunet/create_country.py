from loguru import logger
from geonames import merge_geo

def create_country(country, zone, SESSION):
    """
    Search for the country in Geonames, obtain its hierarchy,
    and create nodes and relationships for each parent.
    """
    logger.info(f"CREATE country node ({country})")

    # Use the geoname ID to get the country's hierarchy
    merge_geo(country, SESSION)

    SESSION.run(
    f'MATCH  (zone:TransmissionZone {{name: "{zone}"}}) '
    f'MERGE (g:Geography {{name: "{country}"}})-[:IN]->(zone) '
)
