from loguru import logger
from flunet.valid_flunet import valid_flunet
from geonames.merge_geo import merge_geo


def process_geographies(session) -> None:
    unrevised_geographies = """
    MATCH (g:Geography)
    WHERE g.geonameId IS NULL
    RETURN id(g) AS id, g.name AS name
    """
    result = session.run(unrevised_geographies)
    result = {data[0]: data[1] for data in result}
    for node_id, name in result.items():
        merge_geo(name, node_id, session)


def ingest_flunet(session) -> None:
    influenza_a, influenza_b = valid_flunet()
    query = """
    UNWIND $Mapping AS mapping
    MERGE (flunet:FluNet:Report {reportId : mapping.reportId})
    MERGE (event:Event {eventId : mapping.eventId,
        startDate : mapping.startDate, 
        endDate : mapping.endDate})
    MERGE (host:Taxon {dataSource : 'NCBI Taxonomy',
        name : 'Homo sapiens',
        rank : 'Species',
        taxId : 9606})
    MERGE (pathogen:Taxon {role : 'pathogen', 
        name : mapping.type})
    MERGE (territory:Geography {name : mapping.Territory})
    MERGE (flunet)-[:REPORTS]->(event)
    MERGE (event)-[:INVOLVES {role : 'host',
        caseCount:mapping.caseCount}]->(host)
    MERGE (event)-[:INVOLVES {role : 'pathogen',   
        collected : mapping.Collected, 
        processed : mapping.Processed, 
        positive : mapping.caseCount}]->(pathogen)
    MERGE (event)-[:OCCURS_IN]->(territory)
    """

    logger.info("Running on influenza A nodes")
    session.run(query, Mapping=influenza_a)
    logger.info("Running on influenza B nodes")
    session.run(query, Mapping=influenza_b)
    process_geographies(session)
