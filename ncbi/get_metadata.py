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

    if not soup.TaxaSet:
        raise ValueError("TaxaSet object not found in the API response.")

    taxon_set = soup.TaxaSet

    if not taxon_set.Taxon:
        raise ValueError("Taxon object not found in the API response.")

    taxon = taxon_set.Taxon

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
        "dataSource": "NCBI Taxonomy"
    }

    if taxon.OtherNames:
        taxon_metadata["otherNames"] = (taxon.OtherNames.getText(),)

    # parse lineage
    lineage_ex = []
    if taxon.LineageEx and taxon.LineageEx.children:
        for taxon_child in taxon.LineageEx.children:
            if isinstance(taxon_child, Tag):
                lineage_ex.append(
                    {
                        "taxId": taxon_child.TaxId.getText(),
                        "scientificName": taxon_child.ScientificName.getText(),
                        "rank": taxon_child.Rank.getText(),
                        "dataSource": "NCBI Taxonomy"
                    }
                )
            time.sleep(SLEEP_TIME)
    taxon_metadata["lineageEx"] = lineage_ex

    return taxon_metadata
