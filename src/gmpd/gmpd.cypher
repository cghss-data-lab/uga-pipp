UNWIND $Mapping AS mapping
MERGE (gmpd:GMPD:Report {reference : mapping.Citation})


// Process host information
FOREACH (map in (CASE WHEN mapping.Host.taxId IS NOT NULL THEN [1] ELSE [] END) |
    MERGE (host:Taxon {taxId : mapping.Host.taxId})
    ON CREATE SET
        host.name = mapping.Host.name,
        host.rank = mapping.Host.rank,
        host.dataSource = "NCBI"
    MERGE (gmpd)-[:ASSOCIATES {role : 'host'}]->(host))

FOREACH (map in (CASE WHEN mapping.Host.taxId IS NULL THEN [1] ELSE [] END) |
    MERGE (host:Taxon {name : mapping.HostCorrectedName})
    MERGE (gmpd)-[:ASSOCIATES {role : 'host'}]->(host))


// Process pathogen information
FOREACH (map in (CASE WHEN mapping.Parasite.taxId IS NOT NULL THEN [1] ELSE [] END) |
    MERGE (pathogen:Taxon {taxId : mapping.Parasite.taxId})
    ON CREATE SET
        pathogen.name = mapping.Parasite.name,
        pathogen.rank = mapping.Parasite.rank,
        pathogen.dataSource = "NCBI"
    MERGE (gmpd)-[:ASSOCIATES {role : 'pathogen'}]->(pathogen))

FOREACH (map in (CASE WHEN mapping.Parasite.taxId IS NULL THEN [1] ELSE [] END) |
    MERGE (pathogen:Taxon {name : mapping.ParasiteCorrectedName})
    MERGE (gmpd)-[:ASSOCIATES {role : 'pathogen'}]->(pathogen))


// Process geographical location 
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
        territory.fcode = mapping.location.fcode
    MERGE (gmpd)-[:ABOUT]->(territory))

FOREACH (map in (CASE WHEN mapping.location.geonameId IS NULL THEN [1] ELSE [] END) | 
    MERGE (territory:Geography {name : mapping.LocationName})
    MERGE (gmpd)-[:ABOUT]->(territory))