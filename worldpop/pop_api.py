import requests
from bs4 import BeautifulSoup


# TO DO: Doesn't seem to be publicly advertised, but found this.
# Can't figure out how to search by location name and return ID
# FYI : https://population.un.org/dataportalapi/index.html

# def pop_api(indicators, locations, params):
#     url = f"https://population.un.org/dataportalapi/api/v1/data/indicators/{indicators}/locations/{locations}&format=json"
#     response = requests.get(url, params)
#     soup = BeautifulSoup(response.content, features="xml")

#     return soup
