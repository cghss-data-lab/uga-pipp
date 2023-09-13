from datetime import datetime
from loguru import logger
from worldpop import get_rows
from ncbi.ncbi_api import NCBI
from ncbi.merge_taxon import merge_taxon
from geonames.merge_geo import merge_geo

ncbi_api = NCBI()

HUMAN_TAXID = 9606
DATA_SOURCE = "UN World Population Prospects 2022"


def search_and_merge(tax_id, session):
    logger.info(f"CREATING node {tax_id}")
    ncbi_metadata = ncbi_api.get_metadata(tax_id)
    taxon = {**ncbi_metadata, "taxId": tax_id}
    merge_taxon(taxon, session)


def ingest_worldpop(session, estimate=False):
    pop_rows = get_rows()  # Tax ID for humans
    search_and_merge(HUMAN_TAXID, session)
    for index, row in enumerate(pop_rows):
        iso2 = row["ISO2_code"]
        if iso2:
            logger.info(f"Creating Population for {iso2}")
            # Fields in thousands were multiplied
            # Rates are per 1000

            reportId = "WorldPop-" + str(index)
            year = int(row["Time"])
            # Assume year is an integer variable containing the year value
            if year >= 2022:
                estimate = True

            raw_date = datetime(year, 7, 1)
            date = raw_date.strftime("%Y-%m-%d")
            totalPopulation = float(row["TPopulation1July"]) * 1000
            # totalMalePop = (float(row['TPopulationMale1July'])*1000)
            # totalFemalePop = (float(row['TPopulationFemale1July'])*1000)
            # popDensity = float(row["PopDensity"])
            # popSexRatio = float(row["PopSexRatio"])
            medianAge = float(row["MedianAgePop"])
            naturalChange = float(row["NatChange"]) * 1000
            # naturalChangeRate = (float(row["NatChangeRT"]))
            populationChange = float(row["PopChange"]) * 1000
            # populationGrowthRate = (float(row["PopGrowthRate"]))
            # pop_doubling = (float(row["DoublingTime"]))
            births = float(row["Births"]) * 1000
            # crudeBirthRate = float(row["CBR"])
            # totalFertilityRate = float(row["TFR"])
            # netReproductionRate = float(row["NRR"])
            # mean_age_childbearing = float(row["MAC"])
            deaths = float(row["Deaths"]) * 1000
            # male_deaths = float(row["MaleDeaths"]*1000)
            # female_deaths = float(row["FemaleDeaths"]*1000)
            # crudeDeathRate = float(row["CDR"])
            lifeExpectancy = float(row["LEx"])
            # life_expectancy_male = float(row["LExMale"])
            # life_expectancy_female = float(row["LExFemale"])
            infantDeaths = float(row["InfantDeaths"]) * 1000
            # infantMortalityRate = float(row["IMR"])
            underFiveDeaths = float(row["Under5Deaths"]) * 1000
            # underFiveMortalityRate = float(row["Q5"])
            # under_40_mortality_rate = float(row["Q0040"])
            netMigration = float(row["NetMigrations"]) * 1000
            # netMigrationRate = float(row["CNMR"])
            duration = "P1Y"  # set duration to 1 year

            parameters = {
                "dataSource": DATA_SOURCE,
                "reportId": reportId,
                "date": date,
                "duration": duration,
                "totalPopulation": totalPopulation,
                "medianAge": medianAge,
                "naturalChange": naturalChange,
                "populationChange": populationChange,
                "births": births,
                "deaths": deaths,
                "lifeExpectancy": lifeExpectancy,
                "infantDeaths": infantDeaths,
                "underFiveDeaths": underFiveDeaths,
                "netMigration": netMigration,
                "estimate": estimate,
            }

            # Create the GeoNames country if it doesn't exist
            merge_geo(row["Location"], session)

            # Match Taxon node to population on TaxId and connect to Geo through Pop
            session.run(
                f"MATCH (t:Taxon {{taxId: {HUMAN_TAXID}}}) "
                f"MERGE (p:Population {{taxId: {HUMAN_TAXID}}}) "
                f"ON CREATE SET p = $props "
                f"ON MATCH SET p += $props "
                f"MERGE (p)-[:COMPRISES]->(t) "
                f'MERGE (g:Geography {{iso2: "{iso2}"}}) '
                f"MERGE (p)-[:INHABITS]->(g)",
                props=parameters,
            )
