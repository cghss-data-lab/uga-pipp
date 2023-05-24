import requests

def get_evolution(eventId):
    evolution_url = f"https://wahis.woah.org/api/v1/pi/event/{eventId}/report-evolution?language=en"
    evolution_response = requests.get(evolution_url)
    data = evolution_response.json()
    evolution_list = []
    if data:
        for key in data:
            reportId = int(data[key]["reportId"])
            evolution_list.append(reportId)

        return evolution_list
    else:
        return None