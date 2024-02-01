UNWIND $Mapping as mapping
MERGE (geo:Geography {geoname_id : mapping.geonameId})
ON CREATE SET 
    geo.name = mapping.name,
    geo.admin_type = mapping.adminType,
    geo.iso2 = mapping.iso2,
    geo.fcl_name = mapping.fclName,
    geo.fcode_name = mapping.fcodeName,
    geo.lat = toFloat(mapping.lat),
    geo.long = toFloat(mapping.lng),
    geo.fcode = mapping.fcode
WITH COLLECT(geo) AS hierarchy
UNWIND RANGE(0, SIZE(hierarchy) - 2) as idx
WITH hierarchy[idx] AS h1, hierarchy[idx+1] AS h2
MERGE (h1)-[:CONTAINS_GEO]->(h2)
