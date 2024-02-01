UNWIND $Mapping AS mapping
CREATE (population:Population {
    data_source : mapping.data_source,
    report_id : mapping.report_id,
    date : DATE(mapping.date),
    duration : mapping.duration,
    totalPopulation : mapping.TPopulation1July,
    medianAge : mapping.MedianAgePop,
    naturalChange : mapping.NatChange,
    populationChange : mapping.PopChange,
    births : mapping.Births,
    deaths : mapping.Deaths,
    lifeExpectancy : mapping.LEx,
    infantDeaths : mapping.InfantDeaths,
    underFiveDeaths : mapping.Under5Deaths,
    netMigration : mapping.NetMigrations})
MERGE (geography:Geography {geoname_id: mapping.geonames.geonameId})
MERGE (population)-[:INHABITS]->(geography)
