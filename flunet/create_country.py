from loguru import logger


def create_transmission_zone(zone, SESSION):
    """
    For now just creates a TransmissionZone node alone, but
    should be expanded to tie that node into the GeoNames system
    """
    logger.info(f" CREATE transmission zone node ({zone})")
    SESSION.run(f'CREATE (n:TransmissionZone:Geo {{name: "{zone}"}})')
