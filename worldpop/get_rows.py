import csv

def get_rows():
    rows = []
    with open("worldpop/data/WPP2022_Demographic_Indicators_Medium.csv", encoding='utf-8') as gmpd_file:
        rows = list(csv.DictReader(gmpd_file))

    return rows