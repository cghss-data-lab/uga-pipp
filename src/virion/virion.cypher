UNWIND $Mapping AS mapping
CREATE (virion:Virion:Report {dataSource : "Virion",
    reportId : mapping.reportId,
    reportDate : mapping.report_date,
    collectionDate : mapping.collection_date,
    ncbiAccession : mapping.ncbi_accession})

MERGE (host:Taxon {taxId : mapping.HostTaxID})
ON CREATE SET
    host.name = mapping.host.name,
    host.rank = mapping.host.rank,
    host.dataSource = "NCBI Taxonomy"

MERGE (pathogen:Taxon {taxId : mapping.VirusTaxID})
ON CREATE SET
    pathogen.name = mapping.pathogen.name,
    pathogen.rank = mapping.pathogen.rank,
    pathogen.dataSource = "NCBI Taxonomy"

MERGE (virion)-[:ASSOCIATES {role : "host"}]->(host)
MERGE (virion)-[:ASSOCIATES {role : "pathogen",
    detectionType : mapping.DetectionMethod}]->(pathogen)  