UNWIND $Mapping AS mapping
CREATE (virion:Report {data_source : "Virion",
    reportId : mapping.reportId,
    reportDate : DATE(mapping.report_date),
    collectionDate : DATE(mapping.collection_date),
    ncbiAccession : mapping.ncbi_accession})

MERGE (host:Taxon {taxId : mapping.HostTaxID})
ON CREATE SET
    host.name = mapping.host.name,
    host.rank = mapping.host.rank,
    host.data_source = "NCBI Taxonomy"

MERGE (pathogen:Taxon {taxId : mapping.VirusTaxID})
ON CREATE SET
    pathogen.name = mapping.pathogen.name,
    pathogen.rank = mapping.pathogen.rank,
    pathogen.data_source = "NCBI Taxonomy"

MERGE (virion)-[:ASSOCIATES {role : "host"}]->(host)
MERGE (virion)-[:ASSOCIATES {role : "pathogen",
    detectionType : mapping.DetectionMethod}]->(pathogen)
 