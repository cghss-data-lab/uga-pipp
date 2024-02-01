UNWIND $Mapping AS mapping
CREATE (gmpd:Report {data_source: "GMPD",
                    reference : mapping.Citation,
                    report_id: mapping.report_id})

CREATE (sample:Sample {data_source: "GMPD"})

MERGE (gmpd)-[:REPORTS]->(sample)
// Process host information
FOREACH (map in (CASE WHEN mapping.Host.taxId IS NOT NULL THEN [1] ELSE [] END) |
    MERGE (host:Taxon {tax_id : mapping.Host.taxId})
    ON CREATE SET
        host.name = mapping.Host.name,
        host.rank = mapping.Host.rank,
        host.data_source = "NCBI Taxonomy"
    MERGE (sample)-[:INVOLVES {role : 'host'}]->(host))

// Process pathogen information
FOREACH (map in (CASE WHEN mapping.Parasite.taxId IS NOT NULL THEN [1] ELSE [] END) |
    MERGE (pathogen:Taxon {tax_id : mapping.Parasite.taxId})
    ON CREATE SET
        pathogen.name = mapping.Parasite.name,
        pathogen.rank = mapping.Parasite.rank,
        pathogen.data_source = "NCBI Taxonomy"
    MERGE (sample)-[:INVOLVES {role : 'pathogen', 
        observation_type : mapping.SamplingType, 
        processed: mapping.processed, 
        positive : mapping.positive
    }]->(pathogen))

// Process geographical location 
FOREACH (map in (CASE WHEN mapping.location.geonameId IS NOT NULL THEN [1] ELSE [] END) |
    MERGE (territory:Geography {geoname_id : mapping.location.geonameId})
    ON CREATE SET 
        territory.data_source = 'GeoNames',
        territory.geoname_id = mapping.location.geonameId,
        territory.name = mapping.location.name,
        territory.admin_type = mapping.location.adminType,
        territory.iso2 = mapping.location.iso2,
        territory.fcl_name = mapping.location.fclName,
        territory.fcode_name = mapping.location.fcodeName,
        territory.lat = toFloat(mapping.location.lat),
        territory.long = toFloat(mapping.location.lng),
        territory.fcode = mapping.location.fcode
    MERGE (sample)-[:OCCURS_IN]->(territory))
