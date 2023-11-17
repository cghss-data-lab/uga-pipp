import csv

def get_rows():
    rows = []
    with open("carnivoreGMPD/data/carnivoreGMPD.csv") as pairings_file:
        rows = list(csv.DictReader(pairings_file))

    return rows