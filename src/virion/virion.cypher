UNWIND $Mapping AS mapping
CREATE (virion:Report {report_id : toInteger(mapping.report_id), 
    report_date : DATE(mapping.report_date), 
    data_source : "Virion"})

CREATE (sample:Sample {data_source : "Virion",
    collection_date : DATE(mapping.collection_date),
    ncbi_accession : mapping.ncbi_accession})

MERGE (host:Taxon {tax_id : toInteger(mapping.HostTaxID)})
ON CREATE SET
    host.name = mapping.host.name,
    host.rank = mapping.host.rank,
    host.data_source = "NCBI Taxonomy"

MERGE (pathogen:Taxon {tax_id : toInteger(mapping.VirusTaxID)})
ON CREATE SET
    pathogen.name = mapping.pathogen.name,
    pathogen.rank = mapping.pathogen.rank,
    pathogen.data_source = "NCBI Taxonomy"

MERGE (virion)-[:REPORTS]->(sample)
MERGE (sample)-[:INVOLVES {role : "host"}]->(host)
MERGE (sample)-[:INVOLVES {role : "pathogen",
    observation_type : mapping.DetectionMethod, 
    observation_date : CASE WHEN mapping.collection_date IS NOT NULL THEN DATE(mapping.collection_date) ELSE "NA" END,
    species_wild : "NA",
    processed : "NA",
    positive: "NA", 
    deaths: "NA" }]->(pathogen)
 
