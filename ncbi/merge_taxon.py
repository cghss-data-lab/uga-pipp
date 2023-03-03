from loguru import logger


# clean taxonomic rank
def sanitize_rank(rank):
    return rank.title().replace(" ", "")



def merge_taxon_node(taxon, SESSION):
    logger.info(f' MERGE node ({taxon["ScientificName"]})')

    rank = sanitize_rank(taxon["Rank"])

    # Use rank, TaxID, and name as merge conditions
    SESSION.run(
        f'MERGE (n:Taxon:{rank} {{name: "{taxon["ScientificName"]}", '
        f'  Rank: "{rank}", '
        f'  TaxId: "{taxon["TaxId"]}" '
        f"}})"
    )


def merge_taxon_link(parent, child, SESSION):
    logger.info(
        f'MERGE link ({parent["ScientificName"]})'
        f'-[:CONTAINS]->({child["ScientificName"]})'
    )

    SESSION.run(
        f'MATCH (parent:Taxon {{TaxId: "{parent["TaxId"]}"}}), '
        f'  (child:Taxon {{TaxId: "{child["TaxId"]}"}}) '
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
    logger.info(f'Merging lineage for {taxon["ScientificName"]}')
    # handle connecting taxon to first parent
    merge_taxon_node(taxon, SESSION)
    merge_taxon_node(taxon["LineageEx"][-1], SESSION)
    merge_taxon_link(taxon["LineageEx"][-1], taxon, SESSION)

    # merge lineage array
    merge_lineage(taxon["LineageEx"], SESSION)
