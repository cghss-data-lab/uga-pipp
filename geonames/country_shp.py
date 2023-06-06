import geopandas as gpd

def country_shp(SESSION):

    # Load the country shapefile from Natural Earth dataset
    countries = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

    # Iterate over the rows of the GeoDataFrame
    for index, row in countries.iterrows():
        iso3 = row['iso_a3']
        if iso3 == "-99":
            continue

        # MATCH node on iso3, set property 'polygon'
        SESSION.run("""
            MATCH (g:Geography {iso3: $iso3})
            SET g.polygon = $polygon
        """, {"iso3": iso3, "polygon": row['geometry'].wkt})


