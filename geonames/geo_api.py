import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv

load_dotenv()

GEO_AUTH = os.getenv("GEO_USER")

def geo_api(service, params):
    """
    Retrieve GeoNames API response, and
    parse it into a beautifulsoup object
    """

    base_url = f"http://api.geonames.org/{service}"
    
    params["username"] = GEO_AUTH

    response = requests.get(base_url, params=params)
    data = response.json()
    return data