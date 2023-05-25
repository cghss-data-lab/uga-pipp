from geonames import geo_api
from loguru import logger
from functools import cache

@cache
def geo_id_search(geoname):
    """
    Searches for a location using its name 
    Returns integer Geonames ID
    """

    logger.info(f"Searching geonames for term {geoname}")
    params = {
        "q": geoname, 
        "maxRows": 1, 
        "fuzzy":0.8
        }

    data = geo_api("searchJSON", params)

    if data.get("totalResultsCount") == 0:
        return None

    geonameId = int(data["geonames"][0]["geonameId"])
    return geonameId

