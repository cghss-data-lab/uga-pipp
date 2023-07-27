import re
import string
from loguru import logger

# from functools import cache
from geonames import geo_api, get_geo_data, geo_id_search
from geonames.cache import cache

# Path to the pickle cache file
ISO_CACHE_FILE = "geonames/iso_cache2.pickle"
GEO_DATA_CACHE_FILE = "geonames/geo_data_cache.pickle"
HIERARCHY_CACHE_FILE = "geonames/hierarchy_cache.pickle"
GEONAMEID_CACHE = "geonames/geonameid_cache.pickle"
FCODE_TO_LABEL = {
    "CONT": "Continent",
    "PCLI": "Country",
    "ADM1": "ADM1",
    "ADM2": "ADM2",
    "ADM3": "ADM3",
    "ADM4": "ADM4",
    "ADM5": "ADM5",
}


@cache(ISO_CACHE_FILE)
def get_iso(iso2):
    parameters = {"country": iso2, "maxRows": 1}
    result = geo_api("countryInfoJSON", parameters)
    return result["geonames"][0]["isoAlpha3"]


@cache(GEO_DATA_CACHE_FILE)
def get_geo_data_cache(geoname_id):
    get_geo_cache = get_geo_data(geoname_id)
    return get_geo_cache


@cache(HIERARCHY_CACHE_FILE)
def get_hierarchy(geoname_id):
    parameters = {"geonameId": geoname_id}
    hierarchy = geo_api("hierarchyJSON", parameters)
    hierarchy_list = hierarchy.get("geonames")
    return hierarchy_list


@cache(GEONAMEID_CACHE)
def get_geonameid(geoname_id):
    return geo_id_search(geoname_id)


def process_parameters(geonameId, metadata, lat, long, iso2) -> dict:
    parameters = {
        "dataSource": "GeoNames",
        "geonameId": int(geonameId),
        "name": metadata.get("name"),
        "adminCode1": metadata.get("adminCodes1", {}).get("ISO3166_2", "NA"),
        "adminType": metadata.get("adminTypeName", "NA"),
        "iso2": metadata.get("countryCode", "NA"),
        "fclName": metadata.get("fclName", "NA"),
        "fcodeName": metadata.get("fcodeName", "NA"),
        "lat": float(lat) if lat is not None else "NA",
        "long": float(long) if long is not None else "NA",
        "elevation": metadata.get("elevation", "NA"),
        "polygon": metadata.get("polygon", "NA"),
    }
    if metadata.get("fcode") == "PCLI":
        parameters["iso3"] = get_iso(iso2)
    if metadata.get("fcode") not in FCODE_TO_LABEL:
        parameters["fcode"] = metadata.get("fcode")
    return parameters


def format_value(value):
    if isinstance(value, str):
        return f'"{value}"'
    if value is None:
        return "NULL"
    return value


def create_properties(parameters: dict) -> str:
    properties = ", ".join(f"{k}: {format_value(v)}" for k, v in parameters.items())
    return properties


# @cache
def merge_geo(geoname_id, session):
    """
    Search for a location by name and return ID, obtain its hierarchy,
    and create nodes and relationships for each parent.
    """
    if isinstance(geoname_id, str):
        geoname_id = get_geonameid(geoname_id)

    # Use the ID to get the location's hierarchy
    hierarchy_list = get_hierarchy(geoname_id)

    if not hierarchy_list:
        return

    nodes = []
    # Create nodes and CONTAINS relationships for each level in the hierarchy
    for i, place in zip(string.ascii_lowercase, hierarchy_list):
        # place = hierarchy_list[i]
        geonameId = place.get("geonameId", None)
        iso2 = place.get("countryCode", None)

        if not geonameId:
            return
        metadata = get_geo_data_cache(geonameId)

        # Modify the latitude and longitude parameters
        lat = metadata.get("lat")
        long = metadata.get("lng")
        parameters = process_parameters(geonameId, metadata, lat, long, iso2)

        if lat is not None and long is not None:
            parameters[
                "location"
            ] = "point({latitude: toFloat($lat), longitude: toFloat($long)})"

        # Get the label for this node based on its fcode value
        fcode = metadata.get("fcode")
        label = "Geography"
        if fcode in FCODE_TO_LABEL:
            label += f":{fcode}"
        parameters = create_properties(parameters)

        # Query
        geo_query = f"MERGE ({i}:{label} {{ {parameters} }})"
        nodes.append(geo_query)

    create_all_nodes_query = "\n".join(nodes)
    merge_all_nodes_query = "-[:CONTAINS_GEO]-".join(
        f"({k})" for k, v in zip(string.ascii_lowercase, nodes)
    )
    logger.info(f"Creating geographical nodes {merge_all_nodes_query}")
    merge_all_nodes_query = re.findall(
        r"(?=(\([a-z]\).*?\([a-z]\)))", merge_all_nodes_query
    )
    merge_all_nodes_query = ["MERGE " + merge for merge in merge_all_nodes_query]
    merge_all_nodes_query = "\n".join(merge_all_nodes_query)

    query = create_all_nodes_query + merge_all_nodes_query

    session.run(query)
