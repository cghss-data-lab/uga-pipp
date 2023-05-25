import os
import json
import requests
from driver.cache import cache


GEOUSER = os.environ["GEOUSER"]


def split_nodes(nodes: list) -> tuple[dict, list]:
    """
    Query returns a list of triplets, main node is repeated.
    This functions splits the node and the adjacent nodes
    """
    flunet_node = None
    adjacent_nodes = []
    for node, adjacent_node, edge_type in nodes:
        if not flunet_node:
            flunet_node = node
        adjacent_nodes.append((adjacent_node, edge_type))
    return flunet_node, adjacent_nodes


@cache
def retrieve_geoname(coord: tuple[float, float]) -> str:
    lat, long = coord[0], coord[1]
    url = f"http://api.geonames.org/findNearbyJSON?lat={lat}&lng={long}&username={GEOUSER}"
    geoname = requests.get(url)
    name = json.dumps(geoname)["name"]
    return name
