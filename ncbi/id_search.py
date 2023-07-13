import ncbi
from loguru import logger
import time
import csv

SLEEP_TIME = 0.4 #3 requests/second

def column_to_ncbi_name(term):
    term_map = {}
    with open("ncbi/data/terms_to_ncbi.csv") as map:
        for row in csv.reader(map):
            orig, ncbi = row
            term_map[orig] = ncbi

    if term in term_map:
        return term_map[term]

    return None
    
def id_search(name):
    """Get ID from text search, using NCBI esearch eutil"""

    logger.info(f"Searching NCBI for term {name}")

    params = {"db": "Taxonomy", "term": name}

    soup = ncbi.api_soup("esearch", params)

    try:
        ncbi_id = soup.find("Id")
        if ncbi_id is not None:
            ncbi_id = ncbi_id.getText()
            time.sleep(SLEEP_TIME)
        else:
            logger.warning("NCBI ID not found for the given term.")
            return None

    except AttributeError:
        errors = soup.find("ErrorList")
        warnings = soup.find("WarningList")

        for error in errors.children:
            logger.error(f"{error.name}: {error.getText()}")

        for warning in warnings.children:
            logger.warning(f"{warning.name}: {warning.getText()}")

        return None

    return ncbi_id


# def id_search(name):
# """Get ID from text search, using NCBI esearch eutil"""

# logger.info(f"Searching NCBI for term {name}")

# params = {"db": "Taxonomy", "term": name}

# soup = ncbi.api_soup("esearch", params)

# try:
#     ncbi_id = soup.find("Id")
#     if ncbi_id is not None:
#         ncbi_id = ncbi_id.getText()
#         time.sleep(SLEEP_TIME)
#     else:
#         ncbi_term = column_to_ncbi_name(name)

#         if ncbi_term is None:
#             logger.warning("NCBI ID not found for the given term.")
#             corrected_name = input("Enter the corrected name: ")

#             with open("ncbi/data/terms_to_ncbi.csv", mode='a') as map:
#                 writer = csv.writer(map)
#                 writer.writerow([name, corrected_name])

#             return id_search(corrected_name)

#         else:
#             logger.info(f"Found a corrected name for term '{name}': {ncbi_term}")
#             return id_search(ncbi_term)

# except AttributeError:
#     errors = soup.find("ErrorList")
#     warnings = soup.find("WarningList")

#     for error in errors.children:
#         logger.error(f"{error.name}: {error.getText()}")

#     for warning in warnings.children:
#         logger.warning(f"{warning.name}: {warning.getText()}")

#     return None

# return ncbi_id
