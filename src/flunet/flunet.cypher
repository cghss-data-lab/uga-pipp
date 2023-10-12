UNWIND $Mapping AS mapping
CREATE (flunet:FluNet:Report {reportId : mapping.reportId})
CREATE (event:Event {eventId : mapping.eventId,
    startDate : mapping.startDate,
    endDate : mapping.endDate})
MERGE (host:Taxon {dataSource : 'NCBI Taxonomy',
    name : 'Homo sapiens',
    rank : 'Species',
    taxId : 9606})
MERGE (pathogen:NoRank:Taxon {taxId : mapping.type})

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
        territory.fcode = mapping.geonames.fcode)

FOREACH (map in (CASE WHEN mapping.geonames.geonameId IS NULL THEN [1] ELSE [] END) |  
    MERGE (territory:Geography {name : mapping.Territory}))

MERGE (flunet)-[:REPORTS]->(event)
MERGE (event)-[:INVOLVES {role : 'host',
    caseCount:mapping.caseCount}]->(host)
MERGE (event)-[:INVOLVES {role : 'pathogen',
    collected : mapping.Collected,
    processed : mapping.Processed,
    positive : mapping.caseCount}]->(pathogen)
MERGE (event)-[:OCCURS_IN]->(territory)
