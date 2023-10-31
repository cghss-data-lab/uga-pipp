UNWIND $Mapping AS mapping
MERGE (population:Population)
ON CREATE SET
    dataSource = mapping.dataSource,
    reportId = mapping.reportId,
    date = mapping.date,
    duration = mapping.duration,
    totalPopulation = mapping.TPopulation1July,
    medianAge = mapping.MedianAgePop,
    naturalChange = mapping.NatChange,
    populationChange = mapping.PopChange,
    births = mapping.Births,
    deaths = mapping.Deaths,
    lifeExpectancy = mapping.LEx,
    infantDeaths = mapping.InfantDeaths,
    underFiveDeaths = mapping.Under5Deaths,
    netMigration = mapping.NetMigrations
MERGE (geography:Geography {geonameId: mapping.geonameId})
MERGE (population)-[:INHABITS]->(geography)
