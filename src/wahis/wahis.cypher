UNWIND $Mapping AS mapping
MERGE (report:Report:WAHIS {uqReportId: mapping.report.uqReportId})
ON CREATE SET 
        dataSource = "WAHIS",
        reportDate = mapping.report.reportedOn,
        reasonForReport = mapping.event.reason.translation,
        description = mapping.event.eventComment
FOREACH (outbreak in mapping.outbreaks | 
        MERGE (event:Event:Outbreak {eventId : outbreak.outbreakId})
        ON CREATE SET
                startDate = outbreak.startDate,
                endDate = outbreak.endDate,
                description = outbreak.description
        
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
        MERGE (host:Taxon {taxId : mapping})
                ON CREATE SET
                host.dataSource = "NCBI Taxonomy",
                host.name = mapping.,
                host.rank = mapping.
        MERGE (event)-[:INVOLVES {role : 'host',
                caseCount : mapping.,
                deathCount : mapping.,
                detectionDate : mapping.
                isWild : mapping}]->(host)

        // Set pathogen informtion
        MERGE (pathogen:Taxon {taxId : mapping})
                ON CREATE SET
                pathogen.dataSource = "NCBI Taxonomy",
                pathogen.name = mapping.,
                pathogen.rank = mapping.
        MERGE (event)-[:INVOLVES {role : 'pathogen'} ]->(pathogen)
)

