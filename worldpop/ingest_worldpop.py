from datetime import datetime
from worldpop import get_rows
from loguru import logger
import ncbi
import geonames

def search_and_merge(taxId, SESSION):
    logger.info(f'CREATING node {taxId}')
    ncbi_metadata = ncbi.get_metadata(taxId)
    taxon = {**ncbi_metadata, "taxId":taxId}
    ncbi.merge_taxon(taxon, SESSION)

def ingest_worldpop(SESSION):
    pop_rows = get_rows()
    taxId = 9606 # Tax ID for humans
    search_and_merge(taxId, SESSION)

    for index, row in enumerate(pop_rows):
        iso2 = row["ISO2_code"]
        if iso2:
            logger.info(f"Creating Population for {iso2}")
            # Fields in thousands were multiplied
            # Rates are per 1000
            dataSource = "UN World Population Prospects 2022"
            dataSourceRow = index
            year = int(row["Time"])
            # Assume year is an integer variable containing the year value
            if year >= 2022:
                estimate = True
            else:
                estimate = False
            raw_date = datetime(year, 7, 1)
            date = raw_date.strftime('%Y-%m-%d')
            totalPopulation = (float(row['TPopulation1July']) * 1000)
            # totalMalePop = (float(row['TPopulationMale1July'])*1000)
            # totalFemalePop = (float(row['TPopulationFemale1July'])*1000)
            # popDensity = float(row["PopDensity"])
            # popSexRatio = float(row["PopSexRatio"])
            medianAge = float(row["MedianAgePop"])
            naturalChange = (float(row["NatChange"])*1000)
            # naturalChangeRate = (float(row["NatChangeRT"]))
            populationChange = (float(row["PopChange"])*1000)
            # populationGrowthRate = (float(row["PopGrowthRate"]))
            # pop_doubling = (float(row["DoublingTime"]))
            births = (float(row["Births"])*1000)
            # crudeBirthRate = float(row["CBR"])
            # totalFertilityRate = float(row["TFR"])
            # netReproductionRate = float(row["NRR"])
            # mean_age_childbearing = float(row["MAC"])
            deaths = (float(row["Deaths"])*1000)
            # male_deaths = float(row["MaleDeaths"]*1000)
            # female_deaths = float(row["FemaleDeaths"]*1000)
            # crudeDeathRate = float(row["CDR"])
            lifeExpectancy = float(row["LEx"])
            # life_expectancy_male = float(row["LExMale"])
            # life_expectancy_female = float(row["LExFemale"])
            infantDeaths = float(row["InfantDeaths"]*1000)
            # infantMortalityRate = float(row["IMR"])
            underFiveDeaths = float(row["Under5Deaths"]*1000)
            # underFiveMortalityRate = float(row["Q5"])
            # under_40_mortality_rate = float(row["Q0040"])
            netMigration = float(row["NetMigrations"]*1000)
            # netMigrationRate = float(row["CNMR"])
            duration = 'P1Y' # set duration to 1 year

            pop_query = """
                MERGE (p:Population {dataSource: $dataSource, 
                                dataSourceRow:$dataSourceRow,
                                date:$date,
                                duration:$duration,
                                totalPopulation:$totalPopulation, 
                                medianAge: $medianAge,
                                naturalChange: $naturalChange,
                                populationChange: $populationChange,
                                births: $births,
                                deaths: $deaths,
                                lifeExpectancy: $lifeExpectancy,
                                infantDeaths: $infantDeaths,
                                underFiveDeaths: $underFiveDeaths,
                                netMigration: $netMigration,
                                estimate: $estimate})
                RETURN p
                """
            
            parameters = {
                "dataSource":dataSource,
                "dataSourceRow":dataSourceRow,
                "date":date,
                "duration":duration,
                "totalPopulation":totalPopulation,
                "medianAge":medianAge,
                "naturalChange":naturalChange,
                "populationChange":populationChange,
                "births":births,
                "deaths":deaths,
                "lifeExpectancy":lifeExpectancy,
                "infantDeaths":infantDeaths,
                "underFiveDeaths":underFiveDeaths,
                "netMigration":netMigration,
                "estimate":estimate
            }

            # Create the GeoNames country if it doesn't exist
            geonames.merge_geo(row["Location"], SESSION)

            # Match Taxon node to population on TaxId and connect to Geo through Pop
            SESSION.run(
                f'MATCH (t:Taxon {{taxId: {taxId}}}) '
                f'MERGE (p:Population {{taxId: {taxId}}}) '
                f'ON CREATE SET p = $props '
                f'ON MATCH SET p += $props '
                f'MERGE (p)-[:COMPRISES]->(t) '
                f'MERGE (g:Geography {{iso2: "{iso2}"}}) '
                f'MERGE (p)-[:INHABITS]->(g)',
                props=parameters
            )



