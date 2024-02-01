import csv
from datetime import datetime

DATA_SOURCE = "UN World Population Prospects 2022"


def valid_worldpop(geo_api) -> list:
    iso_codes = set()
    geonames_iso = []
    worldpop_valid = []
    with open(
        "data/WPP2022_Demographic_Indicators_Medium.csv", "r", encoding="utf-8"
    ) as worldpop_file:
        worldpop = csv.DictReader(worldpop_file)

        for index, row in enumerate(worldpop):
            iso2 = row["ISO2_code"]
            if not iso2:
                continue

            if iso2 not in iso_codes:
                iso_codes.add(iso2)
                geonames_iso.append(geo_api.search_iso(iso2))

            row["data_source"] = DATA_SOURCE

            row["report_id"] = index
            year = int(row["Time"])

            raw_date = datetime(year, 7, 1)
            row["date"] = raw_date.strftime("%Y-%m-%d")
            row["TPopulation1July"] = float(row["TPopulation1July"]) * 1000
            # totalMalePop = (float(row['TPopulationMale1July'])*1000)
            # totalFemalePop = (float(row['TPopulationFemale1July'])*1000)
            # popDensity = float(row["PopDensity"])
            # popSexRatio = float(row["PopSexRatio"])
            row["MedianAgePop"] = float(row["MedianAgePop"])
            row["NatChange"] = float(row["NatChange"]) * 1000
            # naturalChangeRate = (float(row["NatChangeRT"]))
            row["PopChange"] = float(row["PopChange"]) * 1000
            # populationGrowthRate = (float(row["PopGrowthRate"]))
            # pop_doubling = (float(row["DoublingTime"]))
            row["Births"] = float(row["Births"]) * 1000
            # crudeBirthRate = float(row["CBR"])
            # totalFertilityRate = float(row["TFR"])
            # netReproductionRate = float(row["NRR"])
            # mean_age_childbearing = float(row["MAC"])
            row["Deaths"] = float(row["Deaths"]) * 1000
            # male_deaths = float(row["MaleDeaths"]*1000)
            # female_deaths = float(row["FemaleDeaths"]*1000)
            # crudeDeathRate = float(row["CDR"])
            row["LEx"] = float(row["LEx"])
            # life_expectancy_male = float(row["LExMale"])
            # life_expectancy_female = float(row["LExFemale"])
            row["InfantDeaths"] = float(row["InfantDeaths"]) * 1000
            # infantMortalityRate = float(row["IMR"])
            row["Under5Deaths"] = float(row["Under5Deaths"]) * 1000
            # underFiveMortalityRate = float(row["Q5"])
            # under_40_mortality_rate = float(row["Q0040"])
            row["NetMigrations"] = float(row["NetMigrations"]) * 1000
            # netMigrationRate = float(row["CNMR"])
            row["duration"] = "P1Y"  # set duration to 1 year

            worldpop_valid.append(row)

    return worldpop_valid, iso_codes, geonames_iso
