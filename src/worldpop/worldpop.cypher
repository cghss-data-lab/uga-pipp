UNWIND $Mapping AS mapping
CREATE (population:Population {
    data_source : mapping.data_source,
    report_id : mapping.report_id,
    date : DATE(mapping.date),
    total_population : mapping.TPopulation1July,
    median_age : mapping.MedianAgePop,
    natural_change : mapping.NatChange,
    population_change : mapping.PopChange,
    births : mapping.Births,
    deaths : mapping.Deaths,
    life_expectancy : mapping.LEx,
    infant_deaths : mapping.InfantDeaths,
    under_five_deaths : mapping.Under5Deaths,
    net_migration : mapping.NetMigrations})
MERGE (geography:Geography {geoname_id: mapping.geonames.geonameId})
MERGE (population)-[:INHABITS]->(geography)
