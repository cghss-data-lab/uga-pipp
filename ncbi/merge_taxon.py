from loguru import logger


# clean taxonomic rank
def sanitize_rank(rank):
    return rank.title().replace(" ", "")

def merge_taxon_node(taxon, SESSION):
    logger.info(f' MERGE node ({taxon["scientificName"]})')

    rank = sanitize_rank(taxon["rank"])
    taxon_id = int(taxon["taxId"])
    dataSource = "NCBI Taxonomy"

    # Use rank, TaxID, dataSource, and name as merge conditions
    SESSION.run(
        f'MERGE (n:Taxon:{rank} {{name: "{taxon["scientificName"]}", '
        f'  rank: "{rank}", '
        f'  dataSource: "{dataSource}", '
        f'  taxId: {taxon_id} '
        f"}})"
    )


def merge_taxon_link(parent, child, SESSION):
    logger.info(
        f'MERGE link ({parent["scientificName"]})'
        f'-[:CONTAINS]->({child["scientificName"]})'
    )

    parent_taxid = int(parent["taxId"])
    child_taxid = int(child["taxId"])

    SESSION.run(
        f'MATCH (parent:Taxon {{taxId: {parent_taxid}}}), '
        f'  (child:Taxon {{taxId: {child_taxid}}}) '
        f"MERGE (parent)-[:CONTAINS]->(child) "
    )

def merge_lineage(lineage, SESSION):
    for index, taxon in enumerate(lineage):
        # merge parent node
        merge_taxon_node(taxon, SESSION)

        # if there are children
        if index + 1 < len(lineage):
            child = lineage[index + 1]
            # merge child node
            merge_taxon_node(child, SESSION)
            # merge relationship
            merge_taxon_link(taxon, child, SESSION)


def merge_taxon(taxon, SESSION):
    logger.info(f'Merging lineage for {taxon["scientificName"]}')
    # handle connecting taxon to first parent
    merge_taxon_node(taxon, SESSION)
    merge_taxon_node(taxon["lineageEx"][-1], SESSION)
    merge_taxon_link(taxon["lineageEx"][-1], taxon, SESSION)

    # merge lineage array
    merge_lineage(taxon["lineageEx"], SESSION)
