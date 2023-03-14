import csv

def get_rows():
    rows = []
    with open("flunet/data/flunet_1995_2022.csv") as flunet_file:
        rows = list(csv.DictReader(flunet_file))

    return rows
