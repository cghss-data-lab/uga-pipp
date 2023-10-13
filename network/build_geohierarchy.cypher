UNWIND $Mapping as mapping
MERGE (geo:Geography {geonameId : mapping.geonameId})
ON CREATE SET 
    geo.name = mapping.name,
    geo.adminType = mapping.adminType,
    geo.iso2 = mapping.iso2,
    geo.fclName = mapping.fclName,
    geo.fcodeName = mapping.fcodeName,
    geo.lat = toFloat(mapping.lat),
    geo.lng = toFloat(mapping.lng),
    geo.fcode = mapping.fcode
WITH COLLECT(geo) AS hierarchy
UNWIND RANGE(0, SIZE(hierarchy) - 2) as idx
WITH hierarchy[idx] AS h1, hierarchy[idx+1] AS h2
MERGE (h1)-[:CONTAINS_GEO]->(h2)
