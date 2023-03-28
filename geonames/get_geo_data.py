import requests
import os
from dotenv import load_dotenv

load_dotenv()

GEO_AUTH = os.getenv("GEO_USER")

def get_geo_data(search_query):
    """
    Searches for a location in GeoNames and returns a dictionary with its information.
    """
    base_url = "http://api.geonames.org/searchJSON"
    params = {
        "q": search_query, 
        "maxRows": 1, 
        "username": GEO_AUTH, 
        "fuzzy":0.8
        }

    response = requests.get(base_url, params=params)
    data = response.json()

    if data.get("totalResultsCount") == 0:
        return None

    result = data.get("geonames")[0]

    return {
        "name": result.get("name"),
        "countryName": result.get("countryName"),
        "continent": result.get("continentCode"),
        "adminDivision1": result.get("adminCode1"),
        "adminDivision2": result.get("adminCode2"),
        "adminDivision3": result.get("adminCode3"),
        "adminDivision4": result.get("adminCode4"),
        "adminDivision5": result.get("adminCode5"),
        "latitude": result.get("lat"),
        "longitude": result.get("lng"),
        "population": result.get("population")
    }