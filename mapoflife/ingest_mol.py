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

            # Load the country shapefile from Natural Earth dataset
            countries = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

            # Check for intersections with countries
            for index, country in countries.iterrows():
                if wkt_polygon.intersects(country['geometry']):
                    intersecting_area = wkt_polygon.intersection(country['geometry']).area
                    iso3 = country['iso_a3']
                    
                    intersect_query = """
                        MATCH (g1:Geography:speciesRange {polygon: $wkt_polygon}
                        MATCH (g2:Geography {iso3: $iso3})
                        MERGE (g1)-[:INTERSECTS {intersecting_area: $intersecting_area}]->(g2)
                    """
                    SESSION.run(intersect_query, {"wkt_polygon":wkt_polygon,"iso3": iso3, "intersecting_area": intersecting_area})


        
        

        






    




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
