UNWIND $Mapping AS mapping
CREATE (population:Population {
    dataSource : mapping.dataSource,
    reportId : mapping.reportId,
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
MERGE (geography:Geography {geonameId: mapping.geonames.geonameId})
MERGE (population)-[:INHABITS]->(geography)
