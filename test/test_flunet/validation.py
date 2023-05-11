import os
from dotenv import load_dotenv
from neo4j_driver import Neo4jDatabase
from .tests import is_line_null, test_flunet_line_data, is_collected_null
from create_query import create_query_line_data, count_nodes
from write_logs import write_log

load_dotenv()

URI = os.environ["URI"]
AUTH = os.environ["AUTH"]
PASSWORD = os.environ["PASSWORD"]
DATABASE = os.environ["DATABASE"]

neo4j_connection = Neo4jDatabase(URI, DATABASE, AUTH, PASSWORD)


def flunet_validation() -> None:

    with open("./flunet/data/flunet_1995_2022.csv", "r") as flunet:
        header = next(flunet).split(",")
        total = 0
        null = 0
        correct, incorrect = 0, 0
        for row in flunet:
            total += 1  # Count total number of rows
            row = row.split(",")
            # Create a dictionary with the line data
            row_as_dictionary = {k: v for k, v in zip(header, row)}
            # Count and skip empty rows
            if is_line_null(row_as_dictionary):
                null += 1
                continue
            # Query database and test accuracy
            query = create_query_line_data("FluNet", row_as_dictionary[""])
            query_results = neo4j_connection.run_query(query)
            line_data_accuracy = test_flunet_line_data(row_as_dictionary, query_results)

            if all(line_data_accuracy.values()):
                correct += 1  # Count correct values
            else:
                write_log("flunet_validation", row_as_dictionary[""])
                incorrect += 1

            print("T", total, "E", null, "NE", total - null, end="\r")
    return incorrect


def flunet_count() -> bool:
    with open("./flunet/data/flunet_1995_2022.csv", "r") as flunet:
        header = next(flunet).split(",")
        null = 0
        total = 0
        for row in flunet:
            row = row.split(",")
            row_as_dictionary = {k: v for k, v in zip(header, row)}
            if is_collected_null(row_as_dictionary) or is_line_null(row_as_dictionary):
                null += 1
            else:
                write_log("flunet_count", row_as_dictionary[""])
                with open("./test/logs/flunet_validation.log", "a") as log_file:
                    msg = ",".join(row_as_dictionary.values())
                    log_file.write(msg)
            total += 1

    count_query = count_nodes("FluNet")
    result = neo4j_connection.run_query(count_query)

    database_count = dict(result[0])["count(n)"]
    row_count = total - null
    print(database_count, row_count)
    return database_count == row_count
