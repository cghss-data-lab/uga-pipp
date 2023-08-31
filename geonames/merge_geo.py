import re
import string
from loguru import logger
from geonames.geo_api import GeonamesApi
from geonames.process_node import process_node

# from geonames.polygons import search_for_polygon


geonames_api = GeonamesApi()


def multiline_merge_queries(merge_nodes: str) -> str:
    merge_all_nodes_query = re.findall(r"(?=(\([a-z]\).*?\([a-z]\)))", merge_nodes)
    merge_all_nodes_query = ["MERGE " + merge for merge in merge_all_nodes_query]
    merge_all_nodes_query = "\n".join(merge_all_nodes_query)
    return merge_all_nodes_query


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
        parameters, label = process_node(geoname_id, geoname_id)
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

        parameters, label = process_node(geoid, geoname_id)
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
