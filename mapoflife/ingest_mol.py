import geopandas as gpd
from loguru import logger
from shapely.geometry import Polygon, MultiPolygon
from mapoflife import get_rows, mol_search_and_merge

def ingest_mol(SESSION):
    mol_rows = get_rows()

    for dataSourceRow, row in enumerate(mol_rows):
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
                MERGE (t:Taxon {taxId: $taxon_ncbi_id})
                MERGE (g:Geography:speciesRange {polygon: $wkt_polygon})
                MERGE (t)-[:SPANS {dataSource: "Map of Life", reference: $reference}]->(g)
            """
            logger.info(f'MERGE taxon: {scientificName}')
            SESSION.run(query, params)

            # Spatial query for intersecting points
            intersects_query = """
                MATCH (g1:Geography:speciesRange)
                WHERE exists(g1.polygon)
                MATCH (g2:Geography)
                WHERE NOT (g2:speciesRange) AND exists(g2.location)
                WITH g1, g2, ST_Contains(ST_GeomFromText(g1.polygon), ST_PointFromText(g2.location)) AS contains
                WHERE contains = true
                MERGE (g1)-[r:INTERSECTS]->(g2)
                SET r.intersecting_area = ST_Area(ST_Intersection(ST_GeomFromText(g1.polygon), ST_PointFromText(g2.location)))
            """
            batch_size = 1000

            # Get the total count of intersecting points
            count_query = """
                MATCH (g1:Geography:speciesRange)
                WHERE exists(g1.polygon)
                MATCH (g2:Geography)
                WHERE NOT (g2:speciesRange) AND exists(g2.location)
                WITH g1, g2, ST_Contains(ST_GeomFromText(g1.polygon), ST_PointFromText(g2.location)) AS contains
                WHERE contains = true
                RETURN count(*) AS count
            """

            result = SESSION.run(count_query)
            total_count = result.single()["count"]

            # Execute the query in batches
            for offset in range(0, total_count, batch_size):
                batch_query = intersects_query + f"\nSKIP {offset} LIMIT {batch_size}"
                SESSION.run(batch_query)



        
        

        






    




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
