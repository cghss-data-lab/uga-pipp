UNWIND $Mapping AS mapping
MERGE (report:Report:WAHIS {uqReportId: mapping.report.uqReportId})
ON CREATE SET 
        report.dataSource = "WAHIS",
        report.reportDate = mapping.report.reportedOn,
        report.reasonForReport = mapping.event.reason.translation,
        report.reportDescription = mapping.event.eventComment
FOREACH (outbreak in mapping.outbreaks | 
        MERGE (event:Event:Outbreak {eventId : outbreak.outbreakId})
        ON CREATE SET
                event.startDate = outbreak.startDate,
                event.endDate = outbreak.endDate,
                event.description = outbreak.description

        MERGE (report)-[:REPORTS]->(event)
        
        // Set geographical information
        MERGE (territory:Geography {geonameId : outbreak.geonames.geonameId})
        ON CREATE SET
                territory.dataSource = 'GeoNames',
                territory.geonameId = outbreak.geonames.geonameId,
                territory.name = outbreak.geonames.name,
                territory.adminType = outbreak.geonames.adminType,
                territory.iso2 = outbreak.geonames.iso2,
                territory.fclName = outbreak.geonames.fclName,
                territory.fcodeName = outbreak.geonames.fcodeName,
                territory.lat = toFloat(outbreak.geonames.lat),
                territory.lng = toFloat(outbreak.geonames.lng),
                territory.fcode = outbreak.geonames.fcode
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

        FOREACH (map in (CASE WHEN mapping.host.taxId IS NULL THEN [1] ELSE [] END) |
                MERGE (host:Taxon {name : mapping.event.disease.group})
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

        FOREACH (map in (CASE WHEN mapping.pathogen.taxId IS NULL THEN [1] ELSE [] END) |
                MERGE (pathogen:Taxon {name : mapping.event.causalAgent.name})
                MERGE (event)-[:INVOLVES {role : 'pathogen'} ]->(pathogen))
)
