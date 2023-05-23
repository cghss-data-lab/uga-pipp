import ncbi
from bs4 import Tag
import time
from functools import cache
SLEEP_TIME = 0.4

@cache
def get_metadata(ncbi_id):
    """Request metadata by NCBI taxonomy ID, and return cleaned object"""

    params = {"db": "Taxonomy", "id": ncbi_id}
    soup = ncbi.api_soup("efetch", params)

    taxon = soup.TaxaSet.Taxon

    taxon_metadata = {
        "scientificName": taxon.ScientificName.getText(),
        "parentTaxId": taxon.ParentTaxId.getText(),
        "rank": taxon.Rank.getText(),
        "division": taxon.Division.getText(),
        "geneticCode": {"GCId": taxon.GCId.getText(), "GCName": taxon.GCName.getText()},
        "mitoGeneticCode": {
            "MGCId": taxon.MGCId.getText(),
            "MGCName": taxon.MGCName.getText(),
        },
        "lineage": taxon.Lineage.getText(),
        "createDate": taxon.CreateDate.getText(),
        "updateDate": taxon.UpdateDate.getText(),
        "pubDate": taxon.PubDate.getText(),
        "dataSource":"NCBI Taxonomy"
    }

    if taxon.otherNames:
        taxon["otherNames"] = (taxon.OtherNames.getText(),)

    # parse lineage
    lineage_ex = []
    for taxon in taxon.LineageEx.children:
        if isinstance(taxon, Tag):
            lineage_ex.append(
                {
                    "taxId": taxon.TaxId.getText(),
                    "scientificName": taxon.ScientificName.getText(),
                    "rank": taxon.Rank.getText(),
                    "dataSource":"NCBI Taxonomy"
                }
            )
        time.sleep(SLEEP_TIME)
    taxon_metadata["lineageEx"] = lineage_ex

    return taxon_metadata
