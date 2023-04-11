from loguru import logger
from geonames import merge_geo


def create_transmission_zone(zone, SESSION):
    """
    For now just creates a TransmissionZone node alone, but
    should be expanded to tie that node into the GeoNames system
    """
    logger.info(f" CREATE transmission zone node ({zone})")
    # merge_geo(zone, SESSION)

    SESSION.run(f'MERGE (n:TransmissionZone:Geography {{name: "{zone}"}})')
