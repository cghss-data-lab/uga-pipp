import csv


def valid_worldpop():
    worldpop_valid = []
    with open(
        "worldpop/data/WPP2022_Demographic_Indicators_Medium.csv", encoding="utf-8"
    ) as pop_file:
        rows = csv.DictReader(pop_file)

    return rows
