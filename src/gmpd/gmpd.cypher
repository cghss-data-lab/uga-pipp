UNWIND $Mapping AS mapping
MERGE (gmpd:GMPD:Report {reference : mapping.Citation})
MERGE (host:Taxon {name : mapping.HostCorrectedName})
MERGE (pathogen:Taxon {name : mapping.ParasiteCorrectedName})
MERGE (gmpd)-[:ASSOCIATES {role : 'host'}]->(host)
MERGE (gmpd)-[:ASSOCIATES {role : 'pathogen'}]->(pathogen)

FOREACH (map in (CASE WHEN mapping.location.geonameId IS NOT NULL THEN [1] ELSE [] END) |
    MERGE (territory:Geography {geonameId : mapping.location.geonameId})
    ON CREATE SET 
        territory.dataSource = 'GeoNames',
        territory.geonameId = mapping.location.geonameId,
        territory.name = mapping.location.name,
        territory.adminType = mapping.location.adminType,
        territory.iso2 = mapping.location.iso2,
        territory.fclName = mapping.location.fclName,
        territory.fcodeName = mapping.location.fcodeName,
        territory.lat = toFloat(mapping.location.lat),
        territory.lng = toFloat(mapping.location.lng),
        territory.fcode = mapping.location.fcode)

FOREACH (map in (CASE WHEN mapping.location.geonameId IS NULL THEN [1] ELSE [] END) |  
    MERGE (territory:Geography {name : mapping.Territory}))

MERGE (gmpd)-[:ABOUT]->(territory)
