import logging
from test_gmpd.types import Gmpd, GmpdReport
from driver.create_query import create_query_line_data
from driver.errors import DetectionError, AccuracyError, PrevalenceError
from driver.helpers import split_nodes


def validate_gmpd(neo4j_driver) -> None:

    with open("./gmpd/data/GMPD_main.csv", "r") as gmpd:
        logging.debug("GMPD main file opened.")
        header = next(gmpd).split(",")

        for row in gmpd:
            row = row.split(",")
            # Create a dictionary with line data
            row_as_dictionary = {k: v for k, v in zip(header, row)}
            row_number = row_as_dictionary[""]

            try:
                query = create_query_line_data("GMPD", row_number)
                result = neo4j_driver.run_query(query)
                node, adj_nodes = split_nodes(result)
                node = Gmpd(dict(node))
                GmpdReport(
                    row_data=row_as_dictionary,
                    neo4j_node=node,
                    adjacent_nodes=adj_nodes,
                )

            except DetectionError as e:
                logging.error("Error at %d", "", exc_info=e)

            except PrevalenceError as e:
                logging.error("Error at %d", "", exc_info=e)

            except AccuracyError as e:
                logging.error("Error at %d", "", exc_info=e)
