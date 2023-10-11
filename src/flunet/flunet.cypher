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
MERGE (territory:Geography {name : mapping.Territory})
ON CREATE SET territory.geonameId = mapping.geonameId
MERGE (flunet)-[:REPORTS]->(event)
MERGE (event)-[:INVOLVES {role : 'host',
    caseCount:mapping.caseCount}]->(host)
MERGE (event)-[:INVOLVES {role : 'pathogen',
    collected : mapping.Collected,
    processed : mapping.Processed,
    positive : mapping.caseCount}]->(pathogen)
MERGE (event)-[:OCCURS_IN]->(territory)