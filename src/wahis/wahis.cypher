UNWIND $Mapping AS mapping
MERGE (report:Report:WAHIS {uqReportId: mapping.report.uqReportId})
ON CREATE SET 
        dataSource = mapping.dataSource,
        reportDate = mapping.report.reportedOn,
        reasonForReport = mapping.event.reason.translation,
        description = mapping.event.eventComment
FOREACH (outbreak in mapping.outbreaks | 
        MERGE (event:Event:Outbreak {outbreakId : outbreak.outbreakId})
        ON CREATE SET
                startDate = outbreak.startDate,
                endDate = outbreak.endDate,
                description = outbreak.description
        MERGE (territory:Geography {geonameId : outbreak.geoname.geonameId})
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
)
