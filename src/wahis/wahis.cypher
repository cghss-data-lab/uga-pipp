UNWIND $Mapping AS mapping
MERGE (report:Report:WAHIS {uqReportId: mapping.report.uqReportId})
ON CREATE SET 
        dataSource: mapping.dataSource,
        reportDate: mapping.report.reportedOn,
        reasonForReport: mapping.event.reason.translation,
        description: mapping.event.eventComment
