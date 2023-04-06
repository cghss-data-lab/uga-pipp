from loguru import logger
from geonames import geo_id_search
from geonames import geo_api
from geonames import get_geo_data


def merge_geo(geoname_or_id, SESSION):
    """
    Search for a location by name and return ID, obtain its hierarchy,
    and create nodes and relationships for each parent.
    """
    # Determine whether geoname_or_id is a geoname or a geoid
    if isinstance(geoname_or_id, str):
        # Search for the location by name and get its ID
        geoId = geo_id_search(geoname_or_id)
        if not geoId:
            logger.warning(f"Cannot find geoname ID for {geoname_or_id}")
            return
    elif isinstance(geoname_or_id, int):
        geoId = geoname_or_id
    else:
        logger.warning(f"{geoname_or_id} is not a valid geoname or geoname ID")
        return

    # Use the ID to get the location's hierarchy
    params = {"geonameId": geoId}
    hierarchy = geo_api("hierarchyJSON", params)
    hierarchy_list = hierarchy.get("geonames")
    
    # Define query
    geo_query = """
        MERGE (g:Geo {
            geonameId: $geonameId, 
            name: $name, 
            adminCode1: $adminCode1,
            adminType: $adminType,
            countryCode: $countryCode,
            fclName: $fclName, 
            fcode: $fcode,
            fcodeName: $fcodeName,
            lat: $lat, 
            lng: $lng,
            elevation: $elevation
        })
    """

    # Define a dictionary mapping fcode values to node labels
    fcode_to_label = {
        "CONT":"Continent",
        "PCLI": "Country",
        "ADM1": "ADM1",
        "ADM2": "ADM2",
        "ADM3":"ADM3",
        "ADM4":"ADM4",
        "ADM5":"ADM5"
    }

    # Create nodes and CONTAINS relationships for each level in the hierarchy
    if hierarchy_list:
        for i in range(len(hierarchy_list)):
            place = hierarchy_list[i]
            geoId = place.get("geonameId", None)
            if geoId:
                metadata = get_geo_data(geoId)
                params = {
                    "geonameId": int(geoId),
                    "name": metadata.get("name"),
                    "adminCode1": metadata.get("adminCodes1", {}).get("ISO3166_2", "N/A"),
                    "adminType":metadata.get("adminTypeName","N/A"),
                    "countryCode":metadata.get("countryCode","N/A"),
                    "fclName": metadata.get("fclName", None),
                    "fcode":metadata.get("fcode",None),
                    "fcodeName": metadata.get("fcodeName", None),
                    "lat": metadata.get("lat", None),
                    "lng": metadata.get("lng", None),
                    "elevation": metadata.get("elevation", "N/A")
                }
                # Get the label for this node based on its fcode value
                label = fcode_to_label.get(metadata.get("fcode"))
                if label:
                    # Add the label to the node creation query
                    geo_query_with_label = f"{geo_query}\nSET g:{label}"
                    SESSION.run(geo_query_with_label, params)
                else:
                    SESSION.run(geo_query, params)


            # Create relationship to parent, except for first item (Earth)
            if i > 0:
                parent = hierarchy_list[i-1]
                SESSION.run("""
                    MATCH (child:Geo {name: $child_name})
                    MATCH (parent:Geo {name: $parent_name})
                    MERGE (parent)-[:CONTAINS]->(child)
                """, {"child_name": metadata["name"], "parent_name": parent["name"]})
