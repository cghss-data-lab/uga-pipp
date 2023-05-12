import collections
from datetime import datetime
from driver.create_query import count_nodes


def gmpd_validation() -> None:

    with open("./gmpd/GMPD_main.csv", "r") as gmpd:
        header = next(gmpd).split(",")

        for row in gmpd:
            row = row.split(",")
            row_as_dictionary = {key: value for key, value in zip(header, row)}


def gmpd_count(neo4j_driver) -> bool:
    count_query = count_nodes("GMPD")
    result = neo4j_driver.run_query(count_query)

    database_count = len(result)

    with open("./gmpd/data/GMPD_main.csv", "r") as flunet:
        header = next(flunet).split(",")

        file_count = sum(1 for _ in flunet)

    difference = file_count - database_count
    return len(difference)
