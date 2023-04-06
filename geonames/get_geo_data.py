from geonames import geo_id_search
from geonames import geo_api
from loguru import logger

def get_geo_data(geoId):
    """Search by Geonames ID
     Return location metadata"""

    logger.info(f"Searching metadata for ID {geoId}")
    params = {
        "geonameId": geoId, 
        "maxRows": 1
        }

    data = geo_api("getJSON", params)

    return data