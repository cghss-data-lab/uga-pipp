from datetime import datetime
from worldpop import get_rows
from loguru import logger
import ncbi
import geonames

def search_and_merge(TaxId, SESSION):
    logger.info(f'CREATING node {TaxId}')
    ncbi_metadata = ncbi.get_metadata(TaxId)
    taxon = {**ncbi_metadata, "TaxId":TaxId}
    ncbi.merge_taxon(taxon, SESSION)

def ingest_worldpop(SESSION):
    pop_rows = get_rows()
    TaxId = 9606 # Tax ID for humans
    search_and_merge(TaxId, SESSION)

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
            total_pop = (float(row['TPopulation1July']) * 1000)
            total_male_pop = (float(row['TPopulationMale1July'])*1000)
            total_female_pop = (float(row['TPopulationFemale1July'])*1000)
            pop_density_sqkm = float(row["PopDensity"])
            pop_sex_ratio = float(row["PopSexRatio"])
            median_age = float(row["MedianAgePop"])
            # nat_change = (float(row["NatChange"])*1000)
            nat_change_rate = (float(row["NatChangeRT"]))
            # pop_change = (float(row["PopChange"])*1000)
            pop_growth_rate = (float(row["PopGrowthRate"]))
            # pop_doubling = (float(row["DoublingTime"]))
            # births = (float(row["Births"])*1000)
            crude_birth_rate = float(row["CBR"])
            total_fertility_rate = float(row["TFR"])
            net_reproduction_rate = float(row["NRR"])
            # mean_age_childbearing = float(row["MAC"])
            # deaths = float(row["Deaths"]*1000)
            # male_deaths = float(row["MaleDeaths"]*1000)
            # female_deaths = float(row["FemaleDeaths"]*1000)
            crude_death_rate = float(row["CDR"])
            life_expectancy_at_birth = float(row["LEx"])
            # life_expectancy_male = float(row["LExMale"])
            # life_expectancy_female = float(row["LExFemale"])
            # infant_deaths = float(row["InfantDeaths"]*1000)
            infant_mortality_rate = float(row["IMR"])
            # under_5_deaths = float(row["Under5Deaths"]*1000)
            under_5_mortality_rate = float(row["Q5"])
            # under_40_mortality_rate = float(row["Q0040"])
            # net_migration = float(row["NetMigrations"]*1000)
            net_migration_rate = float(row["CNMR"])

            pop_query = """
                MERGE (p:Pop {dataSource: $dataSource, 
                                dataSourceRow:$dataSourceRow,
                                date:date($date),
                                totalPopulation:$total_pop, 
                                totalMalePop:$total_male_pop, 
                                totalFemalePop: $total_female_pop,  
                                popDensity:$pop_density_sqkm,
                                popSexRatio: $pop_sex_ratio,
                                medianAge: $median_age,
                                naturalChangeRate: $nat_change_rate,
                                populationGrowthRate: $pop_growth_rate,
                                crudeBirthRate: $crude_birth_rate,
                                totalFertilityRate: $total_fertility_rate,
                                netReproductionRate: $net_reproduction_rate,
                                crudeDeathRate: $crude_death_rate,
                                lifeExpectancy: $life_expectancy_at_birth,
                                infantMortalityRate: $infant_mortality_rate,
                                underFiveMoralityRate: $under_5_mortality_rate,
                                netMigrationRate: $net_migration_rate,
                                estimate: $estimate})
                RETURN p
                """

            parameters = {
                "dataSource":dataSource,
                "dataSourceRow":dataSourceRow,
                "date":date,
                "total_pop":total_pop,
                "total_male_pop":total_male_pop,
                "total_female_pop":total_female_pop,
                "pop_density_sqkm":pop_density_sqkm,
                "pop_sex_ratio":pop_sex_ratio,
                "median_age":median_age,
                "nat_change_rate":nat_change_rate,
                "pop_growth_rate":pop_growth_rate,
                "crude_birth_rate":crude_birth_rate,
                "total_fertility_rate":total_fertility_rate,
                "net_reproduction_rate":net_reproduction_rate,
                "crude_death_rate":crude_death_rate,
                "life_expectancy_at_birth":life_expectancy_at_birth,
                "infant_mortality_rate":infant_mortality_rate,
                "under_5_mortality_rate":under_5_mortality_rate,
                "net_migration_rate":net_migration_rate,
                "estimate":estimate
            }

            # Create the Population node
            SESSION.run(pop_query, parameters)

            # Create the GeoNames country if it doesn't exist
            geonames.merge_geo(row["Location"], SESSION)

            # Match Taxon node to population on TaxId and connect to Geo through Pop
            SESSION.run(
                f'MATCH (t:Taxon {{TaxId: {TaxId}}}) '
                f'MERGE (p:Pop {{TaxId: {TaxId}}}) '
                f'ON CREATE SET p = $props '
                f'ON MATCH SET p += $props '
                f'MERGE (p)-[:COMPRISES]->(t) '
                f'MERGE (g:Geo {{iso2: "{iso2}"}}) '
                f'MERGE (p)-[:INHABITS]->(g)',
                props=parameters
            )



