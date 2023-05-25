from geonames import geo_id_search
from geonames import geo_api
from loguru import logger
from functools import cache
import time

TIME_SLEEP = 3

@cache
def get_geo_data(geonameId):
    """Search by Geonames ID
     Return location metadata"""

    logger.info(f"Searching metadata for ID {geonameId}")
    params = {
        "geonameId": geonameId, 
        "maxRows": 1
        }
    
    data = geo_api("getJSON", params)

    return data