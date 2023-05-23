import logging
from test_gmpd.types import Gmpd
from test_gmpd.errors import DetectionError


def validate_gmpd(neo4j_driver) -> None:

    with open("./gmpd/data/GMPD_main.csv", "r") as gmpd:
        logging.debug("Flunet main file opened.")
        header = next(gmpd).split(",")

        for row in gmpd:
            try:
                pass
            except DetectionError as e:
                logging.error("Error at %d", "", exc_info=e)
