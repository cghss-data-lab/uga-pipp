import ncbi
from loguru import logger
import time

SLEEP_TIME = 0.4  # 3 requests/second

def subtree_search(id):
    """Get Subtree from text search, using NCBI esearch eutil"""

    logger.info(f"Searching NCBI for ID {id}")

    term = "txid" + str(id) + "[Subtree]"

    params = {"db": "Taxonomy", "term": term}

    soup = ncbi.api_soup("esearch", params)

    try:
        id_list = []
        id_elements = soup.find_all("Id")
        for id_element in id_elements:
            id_list.append(id_element.getText())
            time.sleep(SLEEP_TIME)

        return id_list

    except AttributeError:
        errors = soup.find("ErrorList")
        warnings = soup.find("WarningList")

        for error in errors.children:
            logger.error(f"{error.name}: {error.getText()}")

        for warning in warnings.children:
            logger.warning(f"{warning.name}: {warning.getText()}")

        return None
