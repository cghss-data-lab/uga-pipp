from loguru import logger
from flunet.valid_flunet import valid_flunet
from geonames.merge_geo import merge_geo
from ncbi.merge_taxon import merge_taxon
from ncbi.ncbi_api import NCBI


HUMAN_TAXID = 9606
INFA_TAXID = 11320
INFB_TAXID = 11520

ncbi_api = NCBI()


def process_geographies(session) -> None:
    unrevised_geographies = """
    MATCH (g:Geography)
    RETURN id(g) AS id, g.name AS name
    """
    result = session.run(unrevised_geographies)
    result = {data[0]: data[1] for data in result}
    for node_id, name in result.items():
        merge_geo(name, session, node_id)


def process_taxons(session, tax_id) -> None:
    tax_metadata = ncbi_api.get_metadata(tax_id)
    tax_metadata = {**tax_metadata, "taxId": tax_id}
    merge_taxon(tax_metadata, session)


def ingest_flunet(session) -> None:
    logger.info("Initializing flunet ingest")
    # # NCBI taxonomy
    process_taxons(session, HUMAN_TAXID)
    logger.info("Creating taxa human")
    process_taxons(session, INFA_TAXID)
    logger.info("Creating taxa influenza a")
    process_taxons(session, INFB_TAXID)
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
    MERGE (pathogen:NoRank:Taxon {taxId : mapping.type})
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
