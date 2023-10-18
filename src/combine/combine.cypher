UNWIND $Mapping as mapping
MERGE (tax:Taxon {name : mapping.iucn2020_binomial})
ON MATCH SET
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
    tax. = mapping.,
MERGE (g:BioGeographicalRealm:Geography {name : mapping.realm })
MERGE (t)-[:INHABITS]->(g)







