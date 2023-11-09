UNWIND $Mapping AS mapping
CREATE (flunet:FluNet:Report {reportId : mapping.reportId})
CREATE (event:Event:Outbreaks {eventId : mapping.eventId,
    startDate : mapping.startDate,
    endDate : mapping.endDate,
    collected : mapping.Collected,
    processed : mapping.Processed,
    positive : mapping.caseCount})

MERGE (host:Taxon {taxId : 9606,
    name : 'Homo sapiens',
    rank : 'Species',
    dataSource : 'NCBI Taxonomy'})
MERGE (influenzaA:Taxon {taxId : 11320,
    name : "Influenza A virus",
    rank : "Species",
    dataSource : 'NCBI Taxonomy'})
MERGE (influenzaB:Taxon {taxId : 11520,
    name : "Influenza B virus",
    rank : "Species",
    dataSource : 'NCBI Taxonomy'})
MERGE (influenzaH:Taxon {taxId : 114727,
    name : "H1N1 subtype",
    rank : "Serotype",
    dataSource : 'NCBI Taxonomy'})

MERGE (flunet)-[:REPORTS]->(event)
MERGE (event)-[:INVOLVES {role : 'host',
    caseCount:mapping.caseCount}]->(host)

FOREACH (map in (CASE WHEN mapping.Atotal <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen',
        count : mapping.Atotal}]->(influenzaA))

FOREACH (map in (CASE WHEN mapping.Btotal <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen',
        count : mapping.Btotal}]->(influenzaB))

FOREACH (map in (CASE WHEN mapping.AH1N1 <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen',
        count : mapping.AH1N1}]->(influenzaH))

FOREACH (map in (CASE WHEN mapping.geonames.geonameId IS NOT NULL THEN [1] ELSE [] END) |
    MERGE (territory:Geography {geonameId : mapping.geonames.geonameId})
    ON CREATE SET 
        territory.dataSource = 'GeoNames',
        territory.geonameId = mapping.geonames.geonameId,
        territory.name = mapping.geonames.name,
        territory.adminType = mapping.geonames.adminType,
        territory.iso2 = mapping.geonames.iso2,
        territory.fclName = mapping.geonames.fclName,
        territory.fcodeName = mapping.geonames.fcodeName,
        territory.lat = toFloat(mapping.geonames.lat),
        territory.lng = toFloat(mapping.geonames.lng),
        territory.fcode = mapping.geonames.fcode
    MERGE (event)-[:OCCURS_IN]->(territory))

FOREACH (map in (CASE WHEN mapping.geonames.geonameId IS NULL THEN [1] ELSE [] END) |  
    MERGE (territory:Geography {name : mapping.Territory})
    MERGE (event)-[:OCCURS_IN]->(territory))
