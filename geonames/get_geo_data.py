import requests
from bs4 import BeautifulSoup

def geo_soup(search_query):
    """
    Searches for a location in GeoNames
    Returns a dictionary with its information
    """

    url = "http://api.geonames.org/searchJSON"
    params = {"q": search_query, "maxRows": 1, "username": "your_username_here"}

    response = requests.get(url, params=params)

    soup = BeautifulSoup(response.content, features="html.parser")
    return soup