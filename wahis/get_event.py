from bs4 import BeautifulSoup
import requests
import time
import os
import re
import logging


# All information for event
# Event is a cluster of outbreaks
# https://wahis.woah.org/api/v1/pi/review/event/4073/all-information?language=en

# Diseases
# https://wahis.woah.org/api/v1/pi/disease/first-level-filters?language=en

# Subtypes
# https://wahis.woah.org/api/v1/pi/disease/second-level-filters?language=en

# Report lineage
# https://wahis.woah.org/api/v1/pi/event/4967/report-evolution?language=en

# Outbreak
# https://wahis.woah.org/api/v1/pi/review/report/{report_id}/event-information?language=en

# Gets all of the events and all information
def get_event():
    for i in range(1,5020):
        report_url = f"https://wahis.woah.org/api/v1/pi/review/event/{i}/all-information?language=en"
        response = requests.get(report_url)
        soup = BeautifulSoup(response.content, features="json")
        return soup


