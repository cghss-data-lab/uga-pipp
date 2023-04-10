import csv

def get_rows():
    rows = []
    with open("worldpop/data/WPP2022_Demographic_Indicators_Medium.csv", encoding='utf-8') as pop_file:
        rows = list(csv.DictReader(pop_file))

    return rows