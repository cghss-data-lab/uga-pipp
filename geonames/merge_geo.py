import re
import string
from loguru import logger
from geonames.geo_api import GeonamesApi

# from geonames.polygons import search_for_polygon
LABEL = "Geography"
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


def process_parameters(geoname_id, metadata) -> dict:
    lat = metadata.get("lat")
    long = metadata.get("lng")
    iso2 = metadata.get("countryCode", None)

    parameters = {
        "dataSource": "GeoNames",
        "geonameId": int(geoname_id),
        "name": metadata.get("name"),
        "adminCode1": metadata.get("adminCodes1", {}).get("ISO3166_2", "NA"),
        "adminType": metadata.get("adminTypeName", "NA"),
        "iso2": metadata.get("countryCode", "NA"),
        "fclName": metadata.get("fclName", "NA"),
        "fcodeName": metadata.get("fcodeName", "NA"),
        "lat": float(lat) if lat is not None else "NA",
        "long": float(long) if long is not None else "NA",
        "elevation": metadata.get("elevation", "NA"),
        # "polygon": search_for_polygon(geonameId),
    }
    if metadata.get("fcode") == "PCLI":
        parameters["iso3"] = geonames_api.get_iso(iso2)
    if metadata.get("fcode") not in FCODE_TO_LABEL:
        parameters["fcode"] = metadata.get("fcode")
    return parameters


def format_value(value):
    if isinstance(value, str):
        return f"'{value}'"
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


def process_node(geoname_id, label=LABEL):
    metadata = geonames_api.get_geo_data(geoname_id)
    parameters = process_parameters(geoname_id, metadata)
    parameters = create_properties(parameters)

    fcode = metadata.get("fcode")
    if fcode in FCODE_TO_LABEL:
        fcode = FCODE_TO_LABEL[fcode]
        label += f":{fcode}"

    return parameters, label


def merge_geo(geoname, session, node_id=None):
    """
    Search for a location by name and return ID, obtain its hierarchy,
    and create nodes and relationships for each parent.
    """
    geoname_id = geonames_api.geo_id_search(geoname)
    if not geoname_id:
        return

    if node_id:
        # Set properties for original node
        parameters, label = process_node(geoname_id)
        query = f"""
            MATCH(g:Geography)
            WHERE id(g) = {node_id}
            REMOVE g:Geography
            SET g:{label}
            SET g = {{ {parameters} }}
            RETURN g
        """
        session.run(query)

    # Use the ID to get the location's hierarchy
    hierarchy_list = geonames_api.get_hierarchy(geoname_id)

    nodes = []
    # Create nodes and CONTAINS relationships for each level in the hierarchy
    for i, place in zip(string.ascii_lowercase, hierarchy_list):
        geoid = place.get("geonameId", None)
        if not geoid:
            return

        parameters, label = process_node(geoid)
        geo_query = f"MERGE ({i}:{label} {{ {parameters} }})"
        nodes.append(geo_query)

    create_all_nodes_query = "\n".join(nodes)
    merge_all_nodes_query = "-[:CONTAINS_GEO]-".join(
        f"({k})" for k, _ in zip(string.ascii_lowercase, nodes)
    )
    logger.info(f"Creating geographical nodes {merge_all_nodes_query}")
    merge_all_nodes_query = multiline_merge_queries(merge_all_nodes_query)
    query = create_all_nodes_query + merge_all_nodes_query
    session.run(query)
