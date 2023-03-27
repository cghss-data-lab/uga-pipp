import csv
from functools import cache

import ncbi

# this needs to be refactored to create groups
# of agents for each column instead of linking
# to just one
@cache
def column_to_ncbi_name(key):
    agent_map = {}
    with open("flunet/data/flunet_to_ncbi.csv") as map:
        for row in csv.reader(map):
            flunet, ncbi = row
            agent_map[flunet] = ncbi

    if key in agent_map:
        return agent_map[key]

    return None


@cache
def get_ncbi_id(key):
    ncbi_term = column_to_ncbi_name(key)

    if ncbi_term:
        return int(ncbi.id_search(ncbi_term))

    return None


def get_agent_groups(columns):
    """
    Take the columns of the FluNet CSV, identify
    which ones have agents according to the ncbi
    names CSV, and return the NCBI ID each column
    should link to.

    This needs to be changed later to implement
    agent groups instead of linking to NCBI taxons
    directly.
    """
    agent_groups = {}
    for key in columns:
        ncbi_id = get_ncbi_id(key)
        if ncbi_id:
            agent_groups[key] = ncbi_id

    return agent_groups
