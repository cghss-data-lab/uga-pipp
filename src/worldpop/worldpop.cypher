UNWIND $Mapping AS mapping
CREATE (population:Population {
    data_source : mapping.data_source,
    report_id : mapping.report_id,
    date : DATE(mapping.date),
    total_population : toFloat(mapping.TPopulation1July),
    median_age : toFloat(mapping.MedianAgePop),
    natural_change : toFloat(mapping.NatChange),
    population_change : toFloat(mapping.PopChange),
    births : toFloat(mapping.Births),
    deaths : toFloat(mapping.Deaths),
    life_expectancy : toFloat(mapping.LEx),
    infant_deaths : toFloat(mapping.InfantDeaths),
    under_five_deaths : toFloat(mapping.Under5Deaths),
    net_migration : toFloat(mapping.NetMigrations)})
MERGE (geography:Geography {geoname_id: toInteger(mapping.geonames.geonameId)})
MERGE (population)-[:INHABITS]->(geography)
