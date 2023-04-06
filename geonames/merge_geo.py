from loguru import logger
from geonames import geo_id_search
from geonames import geo_api
from geonames import get_geo_data

def merge_geo(geoname, SESSION):
    """
    Search for a location by Geonames ID, obtain its hierarchy,
    and create nodes and relationships for each parent.
    """

    # Search for the location by name and get its ID
    geoId = geo_id_search(geoname)
    if not geoId:
        logger.warning(f"Cannot find geoname ID for {geoname}")
        return

    # Use the ID to get the location's hierarchy
    params = {"geonameId": geoId}
    hierarchy = geo_api("hierarchyJSON", params)
    hierarchy_list = hierarchy.get("geonames")
    
    # Define query
    geo_query = """
        MERGE (g:Geo {
            name: $name, 
            geonameId: $geonameId, 
            adminName1: $adminName1, 
            toponymName: $toponymName, 
            fclName: $fclName, 
            fcodeName: $fcodeName,
            lat: $lat, 
            lng: $lng
        })
    """

 # Create nodes and CONTAINS relationships for each level in the hierarchy
    for i in range(len(hierarchy_list)):
        place = hierarchy_list[i]
        geoId = place.get("geonameId", None)
        if geoId:
            metadata = get_geo_data(geoId)

            params = {
                "name": metadata.get("name"),
                "geonameId": metadata.get("geonameId", None),
                "adminName1": metadata.get("adminName1", None),
                "toponymName": metadata.get("toponymName", None),
                "fclName": metadata.get("fclName", None),
                "fcodeName": metadata.get("fcodeName", None),
                "lat": metadata.get("lat", None),
                "lng": metadata.get("lng", None)
            }
            SESSION.run(geo_query, params)

# Create relationship to parent, except for first item (Earth)
        if i > 0:
            parent = hierarchy_list[i-1]
            SESSION.run("""
                MATCH (child:Geo {name: $child_name})
                MATCH (parent:Geo {name: $parent_name})
                MERGE (parent)-[:CONTAINS]->(child)
            """, {"child_name": metadata["name"], "parent_name": parent["name"]})

# 