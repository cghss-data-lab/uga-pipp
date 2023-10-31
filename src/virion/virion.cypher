UNWIND $Mapping AS mapping
CREATE (virion:Virion:Report {dataSource : "Virion",
    reportId : mapping.reportId,
    reportDate : mapping.report_date,
    collectionDate : mapping.collection_date,
    ncbiAccession : mapping.ncbi_accession})

MERGE (host:Taxon {taxId : mapping.HostTaxID})
ON CREATE SET
    host.name = mapping.Host.name,
    host.rank = mapping.Host.rank,
    host.dataSource = "NCBI"

MERGE (pathogen:Taxon {taxId : mapping.VirusTaxId})
ON CREATE SET
    pathogen.name = mapping.Pathogen.name,
    pathogen.rank = mapping.Pathogen.rank,
    pathogen.dataSource = "NCBI"

MERGE (virion)-[:ASSOCIATES {role : "host"}]->(host)
MERGE (virion)-[:ASSOCIATES {role : "pathogen",
    detectionType : mapping.DetectionMethod}]->(pathogen)  