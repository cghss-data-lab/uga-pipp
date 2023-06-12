from geonames import geo_api
from loguru import logger
from functools import cache
import os
import pickle

POINT_CACHE = "geonames/point_cache.pickle"

# Load the cache from the pickle file if it exists
point_cache = {}
if os.path.exists(POINT_CACHE):
    with open(POINT_CACHE, "rb") as f:
        point_cache = pickle.load(f)

# Function to save the cache to the pickle file
def save_cache():
    with open(POINT_CACHE, "wb") as f:
        pickle.dump(point_cache, f)

def cache(func):
    def wrapper(lat, long):
        key = (lat, long)
        if key in point_cache:
            geonameId = point_cache[key]
            return geonameId
        else:
            geonameId = func(lat, long)
            if geonameId is not None:
                point_cache[key] = geonameId
                save_cache()
            return geonameId
    return wrapper

@cache
def search_lat_long(lat, long):
    # Use the lat, long to find nearest place
    params = {"lat": lat, "lng": long}
    result = geo_api("findNearbyJSON", params)

    # Check if any results were found
    if not result.get("geonames"):
        logger.warning(f"No nearby places found for lat: {lat}, long: {long}")
        return None

    geonameId = result["geonames"][0]["geonameId"]

    # Return the Geoname ID of the nearest place
    return geonameId
