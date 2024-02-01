UNWIND $Mapping AS mapping
CREATE (virion:Report {report_id : mapping.report_id, reportDate : mapping.report_date})
CREATE (sample:Sample {data_source : "Virion",
    collection_date : DATE(mapping.collection_date),
    ncbi_accession : mapping.ncbi_accession})

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

MERGE (virion)-[:REPORTS]->(sample)
MERGE (sample)-[:INVOLVES {role : "host"}]->(host)
MERGE (sample)-[:INVOLVES {role : "pathogen",
    observation_type : mapping.DetectionMethod}]->(pathogen)
 
