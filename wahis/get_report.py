import requests

def get_report(reportId):
    report_url = f"https://wahis.woah.org/api/v1/pi/review/report/{reportId}/all-information?language=en"
    report_response = requests.get(report_url)
    data = report_response.json()
    return data