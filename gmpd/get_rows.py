import csv

def get_rows():
    rows = []
    with open("gmpd/data/GMPD_main.csv") as gmpd_file:
        rows = list(csv.DictReader(gmpd_file))

    return rows