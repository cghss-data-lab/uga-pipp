# from geonames import geo_id_search
# from geonames import geo_api
from loguru import logger

import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv

load_dotenv()


GEO_AUTH = os.getenv("GEO_USER")

def geo_api(service, params):
    """
    Retrieve GeoNames API response, and
    parse it into a beautifulsoup object
    """

    base_url = f"http://api.geonames.org/{service}"
    
    params["username"] = GEO_AUTH

    response = requests.get(base_url, params=params)
    data = response.json()
    return data

# from geonames import geo_api
from loguru import logger


def geo_id_search(geoname):
    """
    Searches for a location using its name 
    Returns integer Geonames ID
    """

    logger.info(f"Searching geonames for term {geoname}")
    params = {
        "q": geoname, 
        "maxRows": 1, 
        "fuzzy":0.8
        }

    data = geo_api("searchJSON", params)

    if data.get("totalResultsCount") == 0:
        return None

    geoId = int(data["geonames"][0]["geonameId"])
    return geoId


def get_geo_data(geoname):
    """Search by Geonames ID
     Return location metadata"""

    geoId = geo_id_search(geoname)

    logger.info(f"Searching metadata for ID {geoId}")
    params = {
        "geonameId": geoId, 
        "maxRows": 1
        }

    data = geo_api("getJSON", params)

    return data

def get_geo_hierarchy(geoname):
    """Search by Geonames ID
     Return hierarchy"""

    geoId = geo_id_search(geoname)

    logger.info(f"Getting hierarchy for {geoname}")

    params = {
        "geonameId":geoId
    }

    hierarchy = geo_api("hierarchyJSON",params)

    return hierarchy



print(get_geo_hierarchy("Kansas"))


# from neo4j import GraphDatabase
# import requests

# def merge_node(geo, parent_id = None, SESSION):
#     """Merge a geo node in the db using ID
#     Create relationship with parent node"""
#     GeoId = int(geo["GeoId"])

#     query = "MERGE (n {GeoId: $GeoId})"
#     if parent_id is not None:
#         query += f"MATCH (p {{GeoId: {parent_id}}}) MERGE (p)-[:CONTAINS]->(n)"

#     SESSION.run(
#         query,
#         GeoId=geo["GeoId"],
#         props = geo
#     )

# def merge_hierarchy(hierarchy, SESSION):
#     parent_id = None
#     for geo in hierarchy:
#         merge_node(geo, parent_id, SESSION)

#         if parent_id is not None:
#             child_id = geo["GeoId"]
#             merge_node({"GeoId": child_id}, parent_id=parent_id, SESSION)

#         parent_id = geo["GeoId"]

# def merge_geo(geo, SESSION):
#     logger.info(f'Merging hierarchy for {geo["asciiName"]}')
#     # handle connecting geo to first parent
#     merge_node(geo, SESSION)
#     merge_node(geo["LineageEx"][-1], SESSION)
#     merge_taxon_link(taxon["LineageEx"][-1], taxon, SESSION)

#     # merge lineage array
#     merge_lineage(taxon["LineageEx"], SESSION)

