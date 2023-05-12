from .tests import is_line_null, test_flunet_line_data, is_collected_null
from driver.create_query import create_query_line_data, count_nodes
from write_logs import write_log


def flunet_validation(neo4j_driver) -> int:

    with open("./flunet/data/flunet_1995_2022.csv", "r") as flunet:
        header = next(flunet).split(",")
        incorrect = 0
        for row in flunet:
            total += 1  # Count total number of rows
            row = row.split(",")
            # Create a dictionary with the line data
            row_as_dictionary = {k: v for k, v in zip(header, row)}
            # Count and skip empty rows
            if is_collected_null(row_as_dictionary) or is_line_null(row_as_dictionary):
                continue
            # Query database and test accuracy
            query = create_query_line_data("FluNet", row_as_dictionary[""])
            query_results = neo4j_driver.run_query(query)
            line_data_accuracy = test_flunet_line_data(row_as_dictionary, query_results)

            if not all(line_data_accuracy.values()):
                write_log("flunet_validation", row_as_dictionary[""])
                incorrect += 1

    return incorrect


def flunet_count(neo4j_driver) -> bool:
    count_query = count_nodes("FluNet")
    result = neo4j_driver.run_query(count_query)
    result = set([str(dict(r)["n.dataSourceRow"]) for r in result])
    with open("./flunet/data/flunet_1995_2022.csv", "r") as flunet:
        header = next(flunet).split(",")
        csv = set()
        for row in flunet:
            row = row.split(",")
            row_as_dictionary = {k: v for k, v in zip(header, row)}
            if is_collected_null(row_as_dictionary) or is_line_null(row_as_dictionary):
                continue
            else:
                csv.add(row_as_dictionary[""])

        difference = csv ^ result
        for row_number in difference:
            write_log("flunet_count", row_number)
        return len(difference)
