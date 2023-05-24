import requests

def get_outbreak(reportId, eventId):
   outbreak_url = f'https://wahis.woah.org/api/v1/pi/review/report/{reportId}/outbreak/{eventId}/all-information?language=en'
   outbreak_response = requests.get(outbreak_url)
   data = outbreak_response.json()
   return data
