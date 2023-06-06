from loguru import logger
from functools import cache
from geonames import geo_api

@cache
def get_hierarchy(geonameId):
    params = {"geonameId": geonameId}
    hierarchy = geo_api("hierarchyJSON", params)
    hierarchy_list = hierarchy.get("geonames")
    return hierarchy_list