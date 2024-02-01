UNWIND $Mapping AS mapping
MERGE (report:Report {report_id: mapping.report.reportId})
ON CREATE SET 
        report.data_source = "WAHIS",
        report.report_date = DATE(mapping.report.reportedOn),
        report.reason_for_report = mapping.event.reason.translation,
        report.report_description = mapping.event.eventComment

MERGE (event:Event:Outbreak {eventId : mapping.outbreak.outbreakId})
ON CREATE SET
        event.start_date = DATE(mapping.outbreak.start_date),
        event.end_date = DATE(mapping.outbreak.end_date),
        event.description = mapping.outbreak.description

MERGE (report)-[:REPORTS]->(event)

// Set geographical information
MERGE (territory:Geography {geoname_id : mapping.outbreak.geonames.geonameId})
ON CREATE SET
        territory.data_source = 'GeoNames',
        territory.geoname_id = mapping.outbreak.geonames.geonameId,
        territory.name = mapping.outbreak.geonames.name,
        territory.admin_type = mapping.outbreak.geonames.adminType,
        territory.iso2 = mapping.outbreak.geonames.iso2,
        territory.fcl_name = mapping.outbreak.geonames.fclName,
        territory.fcode_name = mapping.outbreak.geonames.fcodeName,
        territory.lat = toFloat(mapping.outbreak.geonames.lat),
        territory.long = toFloat(mapping.outbreak.geonames.lng),
        territory.fcode = mapping.outbreak.geonames.fcode
MERGE (event)-[:OCCURS_IN]->(territory)

// Set host information (mapping.hosts = array of dictionaries)
// Skip host data list if tax ID is null
FOREACH (hostDataList in mapping.hosts |
        FOREACH (hostData in (CASE WHEN hostDataList.taxId IS NOT NULL THEN hostDataList ELSE [] END) |
                MERGE (host:Taxon {taxId: hostData.taxId})
                ON CREATE SET
                        host.name = hostData.name,
                        host.rank = hostData.rank,
                        host.data_source = "NCBI Taxonomy"
                MERGE (event)-[involves:INVOLVES {role: 'host'}]->(host)
                ON CREATE SET
                        involves.processed = hostData.processed,
                        involves.positive = hostData.positive,
                        involves.deaths = hostData.deaths,
                        involves.observation_type = hostData.observation_type,
                        involves.observation_date = DATE(hostData.observation_date),
                        involves.species_wild = hostData.species_wild
        ))

// Process pathogen information
FOREACH (map in (CASE WHEN mapping.pathogen.taxId IS NOT NULL THEN [1] ELSE [] END) |
        MERGE (pathogen:Taxon {taxId : mapping.pathogen.taxId})
        ON CREATE SET
                pathogen.name = mapping.pathogen.name,
                pathogen.rank = mapping.pathogen.rank,
                pathogen.data_source = "NCBI Taxonomy"
        MERGE (event)-[:INVOLVES {role : 'pathogen'} ]->(pathogen))