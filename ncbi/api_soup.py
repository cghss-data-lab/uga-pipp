import requests
from bs4 import BeautifulSoup
import time
SLEEP_TIME=0.4


def api_soup(eutil, params):
    """
    Retrieve NCBI Eutils response XML, and
    parse it into a beautifulsoup object
    """

    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/{eutil}.fcgi"
    response = requests.get(url, params)
    soup = BeautifulSoup(response.content, features="xml")
    time.sleep(SLEEP_TIME)
    return soup