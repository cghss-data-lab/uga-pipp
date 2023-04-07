import requests
from bs4 import BeautifulSoup
import time


def pop_api(indicators, locations, params):
    url = f"https://population.un.org/dataportalapi/api/v1/data/indicators/{indicators}/locations/{locations}&format=json"
    response = requests.get(url, params)
    soup = BeautifulSoup(response.content, features="xml")

    return soup