UNWIND $Mapping AS mapping
CREATE (flunet:Report {reportId : mapping.reportId, data_source: 'FluNet'})
CREATE (event:Event {eventId : mapping.eventId,
    data_source : 'FluNet',
    start_date : DATE(mapping.start_date),
    end_date : DATE(mapping.end_date),
    collected : mapping.Collected,
    processed : mapping.Processed})

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
MERGE (event)-[:INVOLVES {role : 'host'}]->(host)

FOREACH (map in (CASE WHEN mapping.AH1 <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'A(H1)',
        positive : mapping.AH1}]->(influenzaA))

FOREACH (map in (CASE WHEN mapping.AH1N1 <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', 
        positive : mapping.AH1N1}]->(influenzaH))

FOREACH (map in (CASE WHEN mapping.AH3 <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'A(H3)',
        positive : mapping.AH3}]->(influenzaA))

FOREACH (map in (CASE WHEN mapping.AH5 <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'A(H5)',
        positive : mapping.AH5}]->(influenzaA))

FOREACH (map in (CASE WHEN mapping.Anotsubtyped <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'NA',
        positive : mapping.Anotsubtyped}]->(influenzaA))

FOREACH (map in (CASE WHEN mapping.BYamagata <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'Yamagata',
        positive : mapping.BYamagata}]->(influenzaB))

FOREACH (map in (CASE WHEN mapping.BVictoria <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'Victoria',
        positive : mapping.BVictoria}]->(influenzaB))

FOREACH (map in (CASE WHEN mapping.Bnotsubtyped <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'NA',
        positive : mapping.Bnotsubtyped}]->(influenzaB))

FOREACH (map in (CASE WHEN mapping.geonames.geonameId IS NOT NULL THEN [1] ELSE [] END) |
    MERGE (territory:Geography {geoname_id : mapping.geonames.geonameId})
    ON CREATE SET 
        territory.data_source = 'GeoNames',
        territory.geoname_id = mapping.geonames.geonameId,
        territory.name = mapping.geonames.name,
        territory.adminType = mapping.geonames.adminType,
        territory.iso2 = mapping.geonames.iso2,
        territory.fclName = mapping.geonames.fclName,
        territory.fcodeName = mapping.geonames.fcodeName,
        territory.lat = toFloat(mapping.geonames.lat),
        territory.lng = toFloat(mapping.geonames.lng),
        territory.fcode = mapping.geonames.fcode
    MERGE (event)-[:OCCURS_IN]->(territory))
