import ncbi
from functools import cache

@cache
def merge_agent_groups(agent_groups, SESSION):
    """
    Merge the taxons for the agent group into the
    database. Right now it just links to the taxon
    directly, but this needs to be expanded to add
    agent groups so it should merge the group and
    the merge all the taxons in that group.
    """
    for ncbi_id in agent_groups.values():
        ncbi_metadata = ncbi.get_metadata(ncbi_id)
        taxon = {**ncbi_metadata, "TaxId": ncbi_id}
        ncbi.merge_taxon(taxon, SESSION)