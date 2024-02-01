import geopandas as gpd
from loguru import logger
from shapely.geometry import Polygon, MultiPolygon
from mapoflife import get_rows, mol_search_and_merge

def ingest_mol(SESSION):
    mol_rows = get_rows()

    for index, row in enumerate(mol_rows):
        reference = row['citation']

        # Access the polygon/multipolygon and convert to wkt representation
        polygon = row['geometry']
        wkt_polygon = polygon.wkt

        # Get NCBI ID from sciname
        scientificName = row['sciname']
        taxon_ncbi = mol_search_and_merge(scientificName, SESSION)
        if taxon_ncbi:
            taxon_ncbi_id = int(taxon_ncbi)

            params = {
                "reference": reference,
                "taxon_ncbi_id": taxon_ncbi_id,
                "wkt_polygon": wkt_polygon
            }

            # Create taxon (or merge) if there's a match 
            # Taxon species :SPANS over the species range (a type of geography node)
            query = """
                MERGE (t:Taxon {tax_id: $taxon_ncbi_id})
                MERGE (g:Geography:SpeciesRange {polygon: $wkt_polygon})
                MERGE (t)-[:SPANS {data_source: "Map of Life", reference: $reference}]->(g)
            """
            logger.info(f'MERGE taxon: {scientificName}')
            SESSION.run(query, params)

            # # Spatial query for intersecting polygons
            # intersects_query = """
            #     MATCH (g1:Geography:SpeciesRange)
            #     WHERE g1.polygon <> "NA"
            #     MATCH (g2:Geography)
            #     WHERE g2.polygon <> "NA" AND id(g1) <> id(g2)
            #     WITH g1, g2, spatial.intersects(g1.polygon, g2.polygon) AS intersects
            #     WHERE intersects = true
            #     MERGE (g1)-[r:INTERSECTS]->(g2)
            #     SET r.intersecting_area = spatial.area(spatial.intersection(g1.polygon, g2.polygon))
            # """
            # batch_size = 1000

            # count_query = """
            #     MATCH (g1:Geography:SpeciesRange)
            #     WHERE g1.polygon <> "NA"
            #     MATCH (g2:Geography)
            #     WHERE g2.polygon <> "NA" AND id(g1) <> id(g2)
            #     WITH g1, g2, spatial.intersects(g1.polygon, g2.polygon) AS intersects
            #     WHERE intersects = true
            #     RETURN count(*) AS count
            # """

            # result = SESSION.run(count_query)


            # total_count = result.single()["count"]

            # # Execute the query in batches
            # for offset in range(0, total_count, batch_size):
            #     batch_query = intersects_query + f"\nSKIP {offset} LIMIT {batch_size}"
            #     SESSION.run(batch_query)



        
        

        






    




# dataframe = gpd.read_file(shapefile_folder)

# columns = ['sciname','order','family','author','year','citation','rec_source','geometry']


# # Access metadata
# metadata = dataframe._metadata

# # Print metadata
# print(metadata)

# # Access features
# features = dataframe.geometry

# # Iterate over features
# for feature in features:
#     # Access properties of each feature
#     properties = feature.properties
#     # Process properties as needed
#     print(properties)
