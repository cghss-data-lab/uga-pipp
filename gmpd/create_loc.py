from loguru import logger
from geonames import search_lat_lng
from geonames import merge_geo

def create_loc(geoId, SESSION):
    """
    Search for the location in Geonames, obtain its hierarchy,
    and create nodes and relationships for each parent.
    """
    logger.info(f"CREATE geo node ({geoId})")

    # Use the geoname ID to get the country's hierarchy
    merge_geo(geoId, SESSION)
