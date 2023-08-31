import os
import requests
from dotenv import load_dotenv
from loguru import logger
from geonames.cache import cache

load_dotenv()

GEO_AUTH = os.getenv("GEO_USER")
ISO_CACHE_FILE = "geonames/cache/iso_cache.pickle"
GEO_DATA_CACHE_FILE = "geonames/cache/geo_data_cache.pickle"
HIERARCHY_CACHE_FILE = "geonames/cache/hierarchy_cache.pickle"
GEONAMEID_CACHE = "geonames/cache/geonameid_cache.pickle"
POINT_CACHE = "geonames/cache/point_cache.pickle"


class GeonamesApi:
    def __init__(self, geo_auth=GEO_AUTH):
        self.user = geo_auth

    @cache(ISO_CACHE_FILE, is_class=True)
    def get_iso(self, iso2: str) -> str:
        logger.info(f"Searching metadata for iso2 {iso2}")
        parameters = {"country": iso2, "maxRows": 1}
        result = self._geo_api("countryInfoJSON", parameters)
        return result["geonames"][0]["isoAlpha3"]

    @cache(GEO_DATA_CACHE_FILE, is_class=True)
    def get_geo_data(self, geoname_id: str) -> dict:
        """Search by Geonames ID
        Return location metadata"""
        logger.info(f"Searching metadata for ID {geoname_id}")
        parameters = {"geonameId": geoname_id, "maxRows": 1}
        data = self._geo_api("getJSON", parameters)
        return data

    @cache(HIERARCHY_CACHE_FILE, is_class=True)
    def get_hierarchy(self, geoname_id) -> dict:
        logger.info(f"Searching hierarchy for ID {geoname_id}")
        parameters = {"geonameId": geoname_id}
        hierarchy = self._geo_api("hierarchyJSON", parameters)
        hierarchy_list = hierarchy.get("geonames")
        return hierarchy_list

    @cache(GEONAMEID_CACHE, is_class=True)
    def geo_id_search(self, geoname) -> int:
        """
        Searches for a location using its name
        Returns integer Geonames ID
        """
        logger.info(f"Searching geonames for term {geoname}")
        params = {"q": geoname, "maxRows": 1}
        data = self._geo_api("searchJSON", params)

        if data.get("totalResultsCount") == 0:
            return None

        geoname_id = int(data["geonames"][0]["geonameId"])
        return geoname_id

    @cache(POINT_CACHE, is_class=True)
    def search_lat_long(self, point: tuple[float, float]):
        lat, long = point[0], point[1]
        # Use the lat, long to find nearest place
        parameters = {"lat": lat, "lng": long}
        result = self._geo_api("findNearbyJSON", parameters)

        # Check if any results were found
        if not result.get("geonames"):
            logger.warning(f"No nearby places found for lat: {lat}, long: {long}")
            return None

        geoname_id = result["geonames"][0]["geonameId"]
        return geoname_id

    def _geo_api(self, service, parameters):
        """
        Retrieve GeoNames API response, and
        parse it into a beautifulsoup object
        """
        base_url = f"http://api.geonames.org/{service}"

        parameters["username"] = self.user

        response = requests.get(base_url, params=parameters, timeout=1000)
        data = response.json()
        return data
