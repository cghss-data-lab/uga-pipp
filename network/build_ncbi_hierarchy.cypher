UNWIND $Mapping as mapping
MERGE (tax:Taxon {ncbiId : mapping.ncbiId})
ON CREATE SET 
    tax.taxId = mapping.taxId,
    tax.name = mapping.name,
    tax.rank = mapping.rank,
    tax.dataSource = mapping.dataSource,
WITH COLLECT(tax) AS hierarchy
UNWIND RANGE(0, SIZE(hierarchy) - 2) as idx
WITH hierarchy[idx] AS h1, hierarchy[idx+1] AS h2
MERGE (h1)-[:CONTAINS_GEO]->(h2)
