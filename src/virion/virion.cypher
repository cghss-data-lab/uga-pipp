UNWIND $Mapping AS mapping
CREATE (virion:Virion:Report {dataSource : "Virion",
    reportId : mapping.reportId,
    reportDate : mapping.report_date,
    collectionDate : mapping.collection_date,
    ncbiAccession : mapping.ncbi_accession})
MERGE (host:Taxon {taxId : mapping.HostTaxID} )
MERGE (pathogen:Taxon { taxId : mapping.VirusTaxID})
MERGE (virion)-[:ASSOCIATES {role : "host"}]->(host)
MERGE (virion)-[:ASSOCIATES {role : "pathogen",
    detectionType : mapping.DetectionMethod}]->(pathogen)  