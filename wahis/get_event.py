from bs4 import BeautifulSoup
from bs4 import Tag
import requests
from loguru import logger
import json

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
def get_event(eventId):
    report_url = f"https://wahis.woah.org/api/v1/pi/review/event/{event_id}/all-information?language=en"
    response = requests.get(report_url)
    return response


def get_metadata():
    """Request metadata by Event ID, and return cleaned object"""
    for i in range(1,5020):
        eventId = i
        response = get_event(eventId)
        logger.info(f'GETTING event id {eventId}')
        
        data = json.loads(response.content)

        event = data['event']
        eventInCountry = event['isForCountry']
        eventAquatic = event['isAquatic']

        country = event['country']
        countryName = country['name']
        countryCode = country['isoCode']

        disease = event['disease']
        diseaseId = disease['id']
        diseaseName = disease['name']
        diseaseDomestic = disease['isDomestic']
        diseaseAquatic = disease['concernsAquatic']
        
        causalAgent = event['causalAgent']
        agentName = causalAgent['name']

        subtype = event['subtype']
        eventStart = event['startedOn']
        eventConfirmed = event['confirmOn']
        eventEnd = event['endedOn']

        report = data['report']
        reportId = report['reportId']
        previousReportId = report['previousReportId']
        eventReported = report['reportedOn']
        reportReference = report['reportReference']
        reportStatus = report['reportStatus']['keyValue']

        # sources = data['sources']

        # measures = data['measures']
        # for key in measures:
        #     measureId = measures[key]['measureId']
            
        # methods = data['methods']

        outbreaks = data['outbreaks']
        for key in outbreaks:
            outbreakId = outbreaks[key]['id']
            oieReference = outbreaks[key]['oieReference']
            adminDivision = outbreaks[key]['adminDivision']
            location = outbreaks[key]['location']
            lng = outbreaks[key]['longitude']
            lat = outbreaks[key]['latitude']
            clusterCount = outbreaks[key]['clusterCount']
            start = outbreaks[key]['startDate']
            end = outbreaks[key]['endDate']
            linkedReport = outbreaks[key]['createdByReportId']
            desc = outbreaks[key]['description']

        # diagnostics = data['diagnostics']
        quantitativeData = data['quantitativeData']
        new = quantitativeData['news']
        for key in new:
            speciesId = new[key]['speciesId']
            speciesName = new[key]['speciesName']
            speciesWild = new[key]['isWild']
            susceptiblePop = new[key]['susceptible']
            newCases = new[key]['cases']
            newDeaths = new[key]['deaths']
            newKilled = new[key]['killed']
            newSlaughtered = new[key]['slaughtered']
            newVaccinated = new[key]['vaccinated']

        total = quantitativeData['totals']
        for key in total:
            speciesId = total[key]['speciesId']
            speciesName = total[key]['speciesName']
            speciesWild = total[key]['isWild']
            susceptiblePop = total[key]['susceptible']
            totalCases = total[key]['cases']
            totalDeaths = total[key]['deaths']
            totalKilled = total[key]['killed']
            totalSlaughtered = total[key]['slaughtered']
            totalVaccinated = total[key]['vaccinated']













#     event = soup.EventSet.Event

#     event_metadata = {

#         "ScientificName": taxon.ScientificName.getText(),
#         "ParentTaxId": taxon.ParentTaxId.getText(),
#         "Rank": taxon.Rank.getText(),
#         "Division": taxon.Division.getText(),
#         "GeneticCode": {"GCId": taxon.GCId.getText(), "GCName": taxon.GCName.getText()},
#         "MitoGeneticCode": {
#             "MGCId": taxon.MGCId.getText(),
#             "MGCName": taxon.MGCName.getText(),
#         },
#         "Lineage": taxon.Lineage.getText(),
#         "CreateDate": taxon.CreateDate.getText(),
#         "UpdateDate": taxon.UpdateDate.getText(),
#         "PubDate": taxon.PubDate.getText(),
#     }

#     if taxon.otherNames:
#         taxon["OtherNames"] = (taxon.OtherNames.getText(),)

#     # parse lineage
#     lineage_ex = []
#     for taxon in taxon.LineageEx.children:
#         if isinstance(taxon, Tag):
#             lineage_ex.append(
#                 {
#                     "TaxId": taxon.TaxId.getText(),
#                     "ScientificName": taxon.ScientificName.getText(),
#                     "Rank": taxon.Rank.getText(),
#                 }
#             )
#         time.sleep(SLEEP_TIME)
#     taxon_metadata["LineageEx"] = lineage_ex

#     return taxon_metadata


