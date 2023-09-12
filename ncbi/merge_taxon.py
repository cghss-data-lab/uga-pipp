from loguru import logger


# clean taxonomic rank
def sanitize_rank(rank):
    return rank.title().replace(" ", "")


def merge_taxon_node(taxon, session):
    logger.info(f' MERGE node ({taxon["scientificName"]})')

    rank = sanitize_rank(taxon["rank"])
    taxon_id = int(taxon["taxId"])
    data_source = "NCBI Taxonomy"

    # Use rank, TaxID, dataSource, and name as merge conditions
    session.run(
        f"""
        MERGE (n:Taxon:{rank} {{
            name: "{taxon["scientificName"]}", 
            rank: "{rank}",
            dataSource: "{data_source}",
            taxId: {taxon_id}}})
        """
    )


def merge_taxon_link(parent, child, session):
    logger.info(
        f'MERGE link ({parent["scientificName"]})'
        f'-[:CONTAINS_TAXON]->({child["scientificName"]})'
    )

    parent_taxid = int(parent["taxId"])
    child_taxid = int(child["taxId"])

    session.run(
        f"""
        MATCH (parent:Taxon {{taxId: {parent_taxid}}}),
        (child:Taxon {{taxId: {child_taxid}}})
        MERGE (parent)-[:CONTAINS_TAXON]->(child)
        """
    )


def merge_lineage(lineage, session):
    for index, taxon in enumerate(lineage, start=1):
        # merge parent node
        merge_taxon_node(taxon, session)
        # if there are children
        if index < len(lineage):
            child = lineage[index + 1]
            # merge child node
            merge_taxon_node(child, session)
            # merge relationship
            merge_taxon_link(taxon, child, session)


def merge_taxon(taxon, session):
    logger.info(f'Merging lineage for {taxon["scientificName"]}')
    # handle connecting taxon to first parent
    merge_taxon_node(taxon, session)
    merge_taxon_node(taxon["lineageEx"][-1], session)
    merge_taxon_link(taxon["lineageEx"][-1], taxon, session)

    # merge lineage array
    merge_lineage(taxon["lineageEx"], session)
