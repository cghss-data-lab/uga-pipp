import os
import aiohttp
from dotenv import load_dotenv
from loguru import logger
from cache.cache import cache

load_dotenv()

GEO_AUTH = os.getenv("GEO_USER")
GEONAMES_ISO_CACHE_FILE = "network/cache/geonames_iso_cache.pickle"
GEO_DATA_CACHE_FILE = "network/cache/geo_data_cache.pickle"
GEONAMES_HIERARCHY_CACHE_FILE = "network/cache/geonames_hierarchy_cache.pickle"
GEONAMES_ID_CACHE = "network/cache/geonames_id_cache.pickle"
POINT_CACHE = "network/cache/geonames_point_cache_rounded.pickle"


class GeonamesApiError(Exception):
    def __init__(self, value, message):
        self.value = value
        self.message = message
        super().__init__(message)


class GeonamesApi:
    def __init__(self, geo_auth=GEO_AUTH):
        self.user = geo_auth

    @cache(GEONAMES_ISO_CACHE_FILE, is_class=True)
    async def search_iso(self, iso2: str) -> str:
        logger.trace(f"Searching metadata for iso2 {iso2}")
        parameters = {"country": iso2, "maxRows": 1}
        data = await self._geo_api("countryInfoJSON", parameters)
        return self._first_element(data)

    @cache(GEO_DATA_CACHE_FILE, is_class=True)  # unnecessary?
    async def search_geo_data(self, geoname_id: str) -> dict:
        """Search by Geonames ID
        Return location metadata"""
        logger.trace(f"Searching metadata for ID {geoname_id}")
        parameters = {"geonameId": geoname_id, "maxRows": 1}
        data = await self._geo_api("getJSON", parameters)
        return data

    @cache(GEONAMES_HIERARCHY_CACHE_FILE, is_class=True)
    async def search_hierarchy(self, geoname_id) -> dict:
        logger.trace(f"Searching hierarchy for geoname ID {geoname_id}")
        parameters = {"geonameId": geoname_id}
        return await self._geo_api("hierarchyJSON", parameters)

    @cache(GEONAMES_ID_CACHE, is_class=True)
    async def search_geoname_id(self, geoname) -> int:
        """
        Searches for a location using its name
        Returns integer Geonames ID
        """
        logger.trace(f"Searching geonames for term {geoname}")
        params = {"q": geoname, "maxRows": 1}
        data = await self._geo_api("searchJSON", params)
        return self._first_element(data)

    @cache(POINT_CACHE, is_class=True)
    async def search_lat_long(self, point: tuple[float, float]):
        try:
            lat, long = point[0], point[1]
            # Ensure lat and long are float values
            lat = float(lat)
            long = float(long)
        except (ValueError, TypeError):
            # Handle the case where lat or long is not a valid float
            logger.error("Invalid latitude or longitude values provided.")
            return None

        rounded_lat = round(lat, 2)  # Round to 2 decimal places
        rounded_long = round(long, 2)
        logger.trace(f"Searching geonames for location {rounded_lat}, {rounded_long}")
        parameters = {"lat": rounded_lat, "lng": rounded_long}
        data = await self._geo_api("findNearbyJSON", parameters)
        return self._first_element(data)



    async def _geo_api(self, service, parameters):
        """
        Retrieve GeoNames API response
        """
        base_url = f"http://api.geonames.org/{service}"
        parameters["username"] = self.user

        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(base_url, params=parameters) as response:
                result = await response.json()

                if result.get("status"):
                    raise GeonamesApiError(
                        value=parameters,
                        message=result["status"]["message"],
                    )

                if result.get("totalResultsCount") == 0:
                    return None

                return result["geonames"]

    @staticmethod
    def _first_element(element):
        try:
            return element[0]
        except TypeError:
            return element
        except IndexError:
            return None
