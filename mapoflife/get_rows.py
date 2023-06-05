from loguru import logger
import glob
import os
import geopandas as gpd


def get_rows():
    shapefile_folder = '/Users/haileyrobertson/Desktop/HPAI/MDD_Mammalia'

    # For each folder, get the path
    for folder in os.listdir(shapefile_folder):
        folder_path = os.path.join(shapefile_folder, folder)
        
        # Check if it's actually a folder/directory
        if os.path.isdir(folder_path):  
            logger.info(f'GET files for {folder_path}')
            gpkg_files = glob.glob(os.path.join(folder_path, '*.gpkg'))
            
            rows = []

            # For each file in the folder
            for file in gpkg_files:
                data = gpd.read_file(file)
                
                # Convert the data to a list of dictionaries
                for index, row in data.iterrows():
                    row_dict = row.to_dict()
                    rows.append(row_dict)
                
                logger.info(f'APPEND rows from {folder_path}')
                
    return rows

