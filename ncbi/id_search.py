import ncbi
from loguru import logger
import time

SLEEP_TIME = 0.4 #3 requests/second

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
