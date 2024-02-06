UNWIND $Mapping AS mapping
CREATE (flunet:Report {report_id : toInteger(mapping.report_id), data_source: 'FluNet'})
CREATE (event:Event {event_id : mapping.eventId,
    data_source : 'FluNet',
    start_date : DATE(mapping.start_date),
    end_date : DATE(mapping.end_date),
    collected : toFloat(mapping.Collected),
    processed : toFloat(mapping.Processed)})

MERGE (host:Taxon {tax_id : toInteger(9606),
    name : 'Homo sapiens',
    rank : 'species',
    data_source : 'NCBI Taxonomy'})
MERGE (influenzaA:Taxon {tax_id : toInteger(11320),
    name : "Influenza A virus",
    rank : "species",
    data_source : 'NCBI Taxonomy'})
MERGE (influenzaB:Taxon {tax_id : toInteger(11520),
    name : "Influenza B virus",
    rank : "species",
    data_source : 'NCBI Taxonomy'})
MERGE (influenzaH:Taxon {tax_id : toInteger(114727),
    name : "H1N1 subtype",
    rank : "serotype",
    data_source : 'NCBI Taxonomy'})

MERGE (flunet)-[:REPORTS]->(event)
MERGE (event)-[:INVOLVES {role : 'host'}]->(host)

FOREACH (map in (CASE WHEN mapping.AH1 <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'A(H1)',
        positive : toFloat(mapping.AH1), deaths: 'NA', observation_type : "Laboratory detection"}]->(influenzaA))

FOREACH (map in (CASE WHEN mapping.AH1N1 <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', 
        positive : toFloat(mapping.AH1N1), deaths: 'NA', observation_type : "Laboratory detection"}]->(influenzaH))

FOREACH (map in (CASE WHEN mapping.AH3 <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'A(H3)',
        positive : toFloat(mapping.AH3), deaths: 'NA', observation_type : "Laboratory detection"}]->(influenzaA))

FOREACH (map in (CASE WHEN mapping.AH5 <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'A(H5)',
        positive : toFloat(mapping.AH5), deaths: 'NA', observation_type : "Laboratory detection"}]->(influenzaA))

FOREACH (map in (CASE WHEN mapping.Anotsubtyped <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'NA',
        positive : toFloat(mapping.Anotsubtyped), deaths: 'NA', observation_type : "Laboratory detection"}]->(influenzaA))

FOREACH (map in (CASE WHEN mapping.BYamagata <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'Yamagata',
        positive : toFloat(mapping.BYamagata), deaths: 'NA', observation_type : "Laboratory detection"}]->(influenzaB))

FOREACH (map in (CASE WHEN mapping.BVictoria <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'Victoria',
        positive : toFloat(mapping.BVictoria), deaths: 'NA', observation_type : "Laboratory detection"}]->(influenzaB))

FOREACH (map in (CASE WHEN mapping.Bnotsubtyped <> '' THEN [1] ELSE [] END) |
    MERGE (event)-[:INVOLVES {role : 'pathogen', subtype : 'NA',
        positive : toFloat(mapping.Bnotsubtyped), deaths: 'NA', observation_type : "Laboratory detection"}]->(influenzaB))

FOREACH (map in (CASE WHEN mapping.geonames.geonameId IS NOT NULL THEN [1] ELSE [] END) |
    MERGE (territory:Geography {geoname_id : mapping.geonames.geonameId})
    ON CREATE SET 
        territory.data_source = 'GeoNames',
        territory.geoname_id = toInteger(mapping.geonames.geonameId),
        territory.name = mapping.geonames.name,
        territory.admin_type = mapping.geonames.adminType,
        territory.iso2 = mapping.geonames.iso2,
        territory.fcl_name = mapping.geonames.fclName,
        territory.fcode_name = mapping.geonames.fcodeName,
        territory.lat = toFloat(mapping.geonames.lat),
        territory.long = toFloat(mapping.geonames.lng),
        territory.fcode = mapping.geonames.fcode
    MERGE (event)-[:OCCURS_IN]->(territory))
