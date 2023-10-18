from datetime import datetime
from loguru import logger
from src.worldpop.valid_worldpop import valid_worldpop


def ingest_worldpop(database_handler, geoapi):
    pop_rows, iso_codes, geoids = valid_worldpop(geoapi)
