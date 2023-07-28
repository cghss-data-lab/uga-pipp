import re
import string
from loguru import logger
from geonames.geo_api import GeonamesApi

FCODE_TO_LABEL = {
    "CONT": "Continent",
    "PCLI": "Country",
    "ADM1": "ADM1",
    "ADM2": "ADM2",
    "ADM3": "ADM3",
    "ADM4": "ADM4",
    "ADM5": "ADM5",
}

geonames_api = GeonamesApi()


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
        parameters["iso3"] = geonames_api.get_iso(iso2)
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


def multiline_merge_queries(merge_nodes: str) -> str:
    merge_all_nodes_query = re.findall(r"(?=(\([a-z]\).*?\([a-z]\)))", merge_nodes)
    merge_all_nodes_query = ["MERGE " + merge for merge in merge_all_nodes_query]
    merge_all_nodes_query = "\n".join(merge_all_nodes_query)
    return merge_all_nodes_query


def merge_geo(geoname_id, session):
    """
    Search for a location by name and return ID, obtain its hierarchy,
    and create nodes and relationships for each parent.
    """
    if isinstance(geoname_id, str):
        geoname_id = geonames_api.geo_id_search(geoname_id)

    # Use the ID to get the location's hierarchy
    hierarchy_list = geonames_api.get_hierarchy(geoname_id)

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
        metadata = geonames_api.get_geo_data(geonameId)

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
            fcode = FCODE_TO_LABEL[fcode]
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
    merge_all_nodes_query = multiline_merge_queries(merge_all_nodes_query)
    query = create_all_nodes_query + merge_all_nodes_query
    session.run(query)
