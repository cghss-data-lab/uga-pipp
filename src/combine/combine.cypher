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
FOREACH (realm in mapping.biogeographical_realm | 
    MERGE (g:BioGeographicalRealm:Geography {name : realm })
    MERGE (t)-[:INHABITS]->(g)
)







