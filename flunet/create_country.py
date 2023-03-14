from loguru import logger

def create_country(country, zone, SESSION):
    """
    For now, just merges the country into its zone,
    but this should be expanded to use geonames.
    """
    logger.info(f" CREATE country node ({country})")
    SESSION.run(
        f'MATCH  (zone:TransmissionZone {{name: "{zone}"}}) '
        f'MERGE (n:Country:Geo {{name: "{country}"}})-[:IN]->(zone) '
    )
