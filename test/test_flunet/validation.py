import os
from dotenv import load_dotenv
from neo4j_driver import Neo4jDatabase
from .tests import is_line_null, create_query_line_data, test_flunet_line_data

load_dotenv()

URI = os.environ["URI"]
AUTH = os.environ["AUTH"]
PASSWORD = os.environ["PASSWORD"]
DATABASE = os.environ["DATABASE"]

neo4j_connection = Neo4jDatabase(URI, DATABASE, AUTH, PASSWORD)


def run_line_validation():

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

            if is_line_null(row_as_dictionary):
                null += 1  # Count empty rows
                continue

            query = create_query_line_data(row_as_dictionary[""])
            query_results = neo4j_connection.run_query(query)
            line_data_accuracy = test_flunet_line_data(row_as_dictionary, query_results)

            if all(line_data_accuracy.values()):
                correct += 1  # Count correct values
            else:
                with open("./test/logs/flunet_validation.log", "a") as log_file:
                    incorrect += 1
                    msg = "ERROR: " + row_as_dictionary[""] + "\n"
                    log_file.write(msg)

            print(total, end="\r")
        print(
            "Total: ",
            total,
            "Empty: ",
            null,
            "Correct: ",
            correct,
            "Incorrect: ",
            incorrect,
        )
