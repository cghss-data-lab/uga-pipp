from loguru import logger
from functools import cache
from geonames import geo_api

@cache
def get_iso(iso2):
    global iso_cache

    params = {
        "country": iso2,
        "maxRows": 1
    }

    # result = result = geo_api("countryInfoJSON", params)
    # data = result["geonames"][0]["isoAlpha3"]

    if iso2 in iso_cache:
        data = iso_cache[iso2]
    else:
        result = geo_api("countryInfoJSON", params)
        data = result["geonames"][0]["isoAlpha3"]
        iso_cache[iso2] = data
        save_iso_cache()

    return data
