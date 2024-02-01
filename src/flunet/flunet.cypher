UNWIND $Mapping AS mapping
CREATE (flunet:Report {reportId : mapping.reportId})
CREATE (event:Event {eventId : mapping.eventId,
    start_date : DATE(mapping.start_date),
    endDate : DATE(mapping.endDate),
    collected : mapping.Collected,
    processed : mapping.Processed,
    positive : mapping.caseCount})

MERGE (host:Taxon {taxId : 9606,
    name : 'Homo sapiens',
    rank : 'Species',
    data_source : 'NCBI Taxonomy'})
MERGE (influenzaA:Taxon {taxId : 11320,
    name : "Influenza A virus",
    rank : "Species",
    data_source : 'NCBI Taxonomy'})
MERGE (influenzaB:Taxon {taxId : 11520,
    name : "Influenza B virus",
    rank : "Species",
    data_source : 'NCBI Taxonomy'})
MERGE (influenzaH:Taxon {taxId : 114727,
    name : "H1N1 subtype",
    rank : "Serotype",
    data_source : 'NCBI Taxonomy'})

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
        territory.data_source = 'GeoNames',
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
