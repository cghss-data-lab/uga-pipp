import os
import asyncio
import aiohttp
from dotenv import load_dotenv
from loguru import logger
from cache.cache import cache

load_dotenv()

GEO_AUTH = os.getenv("GEO_USER")
ISO_CACHE_FILE = "src/network/cache/iso_cache.pickle"
GEO_DATA_CACHE_FILE = "src/network/cache/geo_data_cache.pickle"
HIERARCHY_CACHE_FILE = "src/network/cache/hierarchy_cache.pickle"
GEONAMEID_CACHE = "src/network/cache/geonameid_cache.pickle"
POINT_CACHE = "src/network/cache/point_cache.pickle"


class GeonamesApi:
    def __init__(self, geo_auth=GEO_AUTH):
        self.user = geo_auth

    @cache(ISO_CACHE_FILE, is_class=True)
    async def search_iso(self, iso2: str) -> str:
        logger.info(f"Searching metadata for iso2 {iso2}")
        parameters = {"country": iso2, "maxRows": 1}
        result = await self._geo_api("countryInfoJSON", parameters)
        return result["geonames"][0]["isoAlpha3"]

    @cache(GEO_DATA_CACHE_FILE, is_class=True)
    async def search_geo_data(self, geoname_id: str) -> dict:
        """Search by Geonames ID
        Return location metadata"""
        logger.info(f"Searching metadata for ID {geoname_id}")
        parameters = {"geonameId": geoname_id, "maxRows": 1}
        data = await self._geo_api("getJSON", parameters)
        return data

    @cache(HIERARCHY_CACHE_FILE, is_class=True)
    async def search_hierarchy(self, geoname_id) -> dict:
        logger.info(f"Searching hierarchy for ID {geoname_id}")
        parameters = {"geonameId": geoname_id}
        hierarchy = await self._geo_api("hierarchyJSON", parameters)
        hierarchy_list = hierarchy.get("geonames")
        return hierarchy_list

    @cache(GEONAMEID_CACHE, is_class=True)
    async def search_geonameid(self, geoname) -> int:
        """
        Searches for a location using its name
        Returns integer Geonames ID
        """
        logger.info(f"Searching geonames for term {geoname}")
        params = {"q": geoname, "maxRows": 1}
        data = await self._geo_api("searchJSON", params)

        if data.get("totalResultsCount") == 0:
            return None

        # geoname_id = int(data["geonames"][0]["geonameId"])
        data = data["geonames"][0]
        return data  # geoname_id

    @cache(POINT_CACHE, is_class=True)
    async def search_lat_long(self, point: tuple[float, float]):
        lat, long = point[0], point[1]
        # Use the lat, long to find nearest place
        parameters = {"lat": lat, "lng": long}
        result = await self._geo_api("findNearbyJSON", parameters)

        # Check if any results were found
        if not result.get("geonames"):
            logger.warning(f"No nearby places found for lat: {lat}, long: {long}")
            return None

        geoname_id = result["geonames"][0]["geonameId"]
        return geoname_id

    async def _geo_api(self, service, parameters):
        """
        Retrieve GeoNames API response
        """
        base_url = f"http://api.geonames.org/{service}"
        parameters["username"] = self.user

        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(base_url, params=parameters) as response:
                return await response.json()
