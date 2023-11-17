UNWIND $Mapping AS mapping
MERGE (report:Report:WAHIS {uqReportId: mapping.report.uqReportId})
ON CREATE SET 
        report.dataSource = "WAHIS",
        report.reportDate = mapping.report.reportedOn,
        report.reasonForReport = mapping.event.reason.translation,
        report.reportDescription = mapping.event.eventComment

MERGE (event:Event:Outbreak {eventId : mapping.outbreak.outbreakId})
ON CREATE SET
        event.startDate = mapping.outbreak.startDate,
        event.endDate = mapping.outbreak.endDate,
        event.description = mapping.outbreak.description

MERGE (report)-[:REPORTS]->(event)

// Set geographical information
MERGE (territory:Geography {geonameId : mapping.outbreak.geonames.geonameId})
ON CREATE SET
        territory.dataSource = 'GeoNames',
        territory.geonameId = mapping.outbreak.geonames.geonameId,
        territory.name = mapping.outbreak.geonames.name,
        territory.adminType = mapping.outbreak.geonames.adminType,
        territory.iso2 = mapping.outbreak.geonames.iso2,
        territory.fclName = mapping.outbreak.geonames.fclName,
        territory.fcodeName = mapping.outbreak.geonames.fcodeName,
        territory.lat = toFloat(mapping.outbreak.geonames.lat),
        territory.lng = toFloat(mapping.outbreak.geonames.lng),
        territory.fcode = mapping.outbreak.geonames.fcode
MERGE (event)-[:OCCURS_IN]->(territory)

// Set host information
FOREACH (map in (CASE WHEN mapping.host.taxId IS NOT NULL THEN [1] ELSE [] END) |
        MERGE (host:Taxon {taxId : mapping.host.taxId})
        ON CREATE SET
                host.name = mapping.host.name,
                host.rank = mapping.host.rank,
                host.dataSource = "NCBI Taxonomy"
        MERGE (event)-[involves:INVOLVES {role : 'host'}]->(host)
        ON CREATE SET
                involves.caseCount = mapping.quantitativeData.totals.cases,
                involves.deathCount = mapping.quantitativeData.totals.deaths,
                //involves.detectionDate = mapping.quantitativeData.totals
                involves.isWild = mapping.quantitativeData.totals.isWild)

// Process pathogen information
FOREACH (map in (CASE WHEN mapping.pathogen.taxId IS NOT NULL THEN [1] ELSE [] END) |
        MERGE (pathogen:Taxon {taxId : mapping.pathogen.taxId})
        ON CREATE SET
                pathogen.name = mapping.pathogen.name,
                pathogen.rank = mapping.pathogen.rank,
                pathogen.dataSource = "NCBI Taxonomy"
        MERGE (event)-[:INVOLVES {role : 'pathogen'} ]->(pathogen))