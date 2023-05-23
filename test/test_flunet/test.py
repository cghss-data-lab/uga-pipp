import logging
from test_flunet.flunet import FluNet, FluNetReport
from dotenv import load_dotenv
from test_flunet.errors import (
    AccuracyError,
    ZeroError,
    TerritoryError,
    DiscrepancyError,
)
from driver.create_query import create_query_line_data

load_dotenv()


def validate_flunet(neo4j_driver) -> None:
    with open("./flunet/data/flunet_1995_2022.csv", "r") as flunet:
        logging.debug("Flunet main file opened.")
        header = next(flunet).split(",")

        for row in flunet:
            # Split data based on csv
            row = row.split(",")
            # Create a dictionary with line data
            row_as_dictionary = {k: v for k, v in zip(header, row)}
            row_number = row_as_dictionary[""]

            if row_as_dictionary["Collected"] in ("", "0"):
                logging.warning(f"Row {row_number} is empty")
                continue

            try:
                query = create_query_line_data("FluNet", row_number)
                result = neo4j_driver.run_query()
                node = FluNet(dict(result))
                FluNetReport(row_as_dictionary, node)

            except AccuracyError as e:
                logging.error("Error at %d", row_as_dictionary[""], exc_info=e)

            except ZeroError as e:
                logging.error("Error at %d", row_as_dictionary[""], exc_info=e)

            except TerritoryError as e:
                logging.error("Error at %d", row_as_dictionary[""], exc_info=e)

            except DiscrepancyError as e:
                logging.error("Error at %d", row_as_dictionary[""], exc_info=e)
