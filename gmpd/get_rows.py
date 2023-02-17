import csv

def get_rows():
    rows = []
    with open("gmpd/data/carnivoreGMPD.csv") as pairings_file:
        rows = list(csv.DictReader(pairings_file))
    
    return rows