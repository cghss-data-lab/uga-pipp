import requests


class WAHIS:
    def get_evolution(self, eventId):
        evolution_url = f"https://wahis.woah.org/api/v1/pi/event/{eventId}/report-evolution?language=en"
        evolution_response = requests.get(evolution_url)
        data = evolution_response.json()
        evolution_list = []
        if data:
            for key in data:
                reportId = key["reportId"]
                evolution_list.append(reportId)

            return evolution_list
        else:
            return None

    def get_outbreak(self, reportId, eventId):
        outbreak_url = f"https://wahis.woah.org/api/v1/pi/review/report/{reportId}/outbreak/{eventId}/all-information?language=en"
        outbreak_response = requests.get(outbreak_url)
        data = outbreak_response.json()
        return data

    def get_report(self, reportId):
        report_url = f"https://wahis.woah.org/api/v1/pi/review/report/{reportId}/all-information?language=en"
        report_response = requests.get(report_url)
        data = report_response.json()
        return data
