from loguru import logger
import glob
import os
import geopandas as gpd
import pickle

def get_rows():
    shapefile_folder = '/Users/haileyrobertson/Desktop/HPAI/MDD_Mammalia'

    # Get the directory path of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Check if the rows pickle file exists
    pickle_file = os.path.join(script_dir, 'rows.pickle')
    if os.path.exists(pickle_file):
        logger.info(f'LOAD rows from {pickle_file}')
        with open(pickle_file, 'rb') as f:
            return pickle.load(f)

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

            # Save the rows to a pickle file in the script directory
            with open(pickle_file, 'wb') as f:
                pickle.dump(rows, f)

            logger.info(f'SAVE rows to {pickle_file}')

            return rows

    return []
