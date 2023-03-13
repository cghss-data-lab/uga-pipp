import csv
from functools import cache


@cache
def get_map():
    agent_map = {}
    with open("flunet/data/flunet_to_ncbi.csv") as map:
        for row in csv.reader(map):
            flunet, ncbi = row
            agent_map[flunet] = ncbi

    return agent_map


def column_to_ncbi(flunet):
    agent_map = get_map()

    if flunet in agent_map:
        return agent_map[flunet]

    return None
