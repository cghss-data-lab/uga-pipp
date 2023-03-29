from loguru import logger
from geonames import geo_id_search
from geonames import geo_api

def get_geo_hierarchy(geoname):
    """Search by Geonames ID
     Return hierarchy"""

    geoId = geo_id_search(geoname)

    logger.info(f"Getting hierarchy for {geoname}")

    params = {
        "geonameId":geoId
    }

    hierarchy = geo_api("hierarchyJSON",params)

    return hierarchy