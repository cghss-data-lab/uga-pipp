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
                territory.geonameId = mapping.geonames.geonameId,
                territory.name = mapping.geonames.name,
                territory.adminType = mapping.geonames.adminType,
                territory.iso2 = mapping.geonames.iso2,
                territory.fclName = mapping.geonames.fclName,
                territory.fcodeName = mapping.geonames.fcodeName,
                territory.lat = toFloat(mapping.geonames.lat),
                territory.lng = toFloat(mapping.geonames.lng),
                territory.fcode = mapping.geonames.fcode
        MERGE (event)-[:OCCURS_IN]->(territory)

        // Set host information
        MERGE (host:Taxon {taxId : mapping.host.taxId})
                ON CREATE SET
                host.dataSource = "NCBI Taxonomy",
                host.name = mapping.host.name,
                host.rank = mapping.host.rank
        MERGE (event)-[:INVOLVES {role : 'host',
                caseCount : mapping.quantitativeData.totals.cases,
                deathCount : mapping.quantitativeData.totals.deaths,
                //detectionDate : mapping.quantitativeData.totals
                isWild : mapping.quantitativeData.totals.isWild}]->(host)

        // Set pathogen informtion
        MERGE (pathogen:Taxon {taxId : mapping.pathogen.taxId})
                ON CREATE SET
                pathogen.dataSource = "NCBI Taxonomy",
                pathogen.name = mapping.pathogen.name,
                pathogen.rank = mapping.pathogen.rank
        MERGE (event)-[:INVOLVES {role : 'pathogen'} ]->(pathogen)
)

