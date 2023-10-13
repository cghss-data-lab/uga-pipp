UNWIND $Mapping AS mapping
MERGE (gmpd:GMPD:Report {reference : mapping.Citation})
MERGE (host:Taxon {name : mapping.HostCorrectedName})
MERGE (pathogen:Taxon {name : mapping.ParasiteCorrectedName})
MERGE (gmpd)-[:ASSOCIATES {role : 'host'}]->(host)
MERGE (gmpd)-[:ASSOCIATES {role : 'pathogen'}]->(pathogen)
FOREACH (geo in mapping.LocationName |
    MERGE (gmpd)-[:ABOUT]->(g:Geography {name : geo}) 
    )