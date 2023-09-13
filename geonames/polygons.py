import re
from cache.cache import cache

POLYGON_CACHE = "geonames/cache/polygon.pickle"


@cache(POLYGON_CACHE)
def search_for_polygon(geoname_id) -> dict:
    with open("./geonames/data/polygonData.txt", "r", encoding="ascii") as polygons:
        pattern = str(geoname_id) + r"(?=.*\{)"
        for polygon in polygons:
            match = re.findall(pattern, polygon)
            if match:
                multipolygon = re.findall(r"\{.*\}", polygon)
                return multipolygon[0]
        return "NA"
