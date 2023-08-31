from loguru import logger
from flunet.valid_flunet import valid_flunet
from geonames.merge_geo import merge_geo
from ncbi import get_metadata, merge_taxon


HUMAN_TAXID = 9606
INFA_TAXID = 11320
INFB_TAXID = 11520


def process_geographies(session) -> None:
    unrevised_geographies = """
    MATCH (g:Geography)
    WHERE g.geonameId IS NULL
    RETURN id(g) AS id, g.name AS name
    """
    result = session.run(unrevised_geographies)
    result = {data[0]: data[1] for data in result}
    for _, name in result.items():
        merge_geo(name, session)


def ingest_flunet(session) -> None:
    logger.info("Initializing flunet ingest")
    # NCBI taxonomy
    human_metadata = get_metadata(HUMAN_TAXID)
    human_metadata = {**human_metadata, "taxId": HUMAN_TAXID}
    merge_taxon(human_metadata, session)
    logger.info("Creating taxa human")
    influenza_a_metadata = get_metadata(INFA_TAXID)
    influenza_a_metadata = {**influenza_a_metadata, "taxId": INFA_TAXID}
    merge_taxon(influenza_a_metadata, session)
    logger.info("Creating taxa influenza a")
    influenza_b_metadata = get_metadata(INFB_TAXID)
    influenza_b_metadata = {**influenza_b_metadata, "taxId": INFB_TAXID}
    merge_taxon(influenza_b_metadata, session)
    logger.info("Creating taxa influenza b")

    influenza_a, influenza_b = valid_flunet()
    query = """
    UNWIND $Mapping AS mapping
    CREATE (flunet:FluNet:Report {reportId : mapping.reportId})
    CREATE (event:Event {eventId : mapping.eventId,
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
