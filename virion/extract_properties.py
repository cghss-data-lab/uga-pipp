from datetime import datetime
from functools import cache
from loguru import logger
from ncbi import get_metadata


def process_dates(year, month, day):
    try:
        date = datetime.strptime(
            f"{year}-{month}-{day}",
            "%Y-%m-%d",
        )
        return date
    except ValueError:
        logger.warning("Incomplete date")
        return "null"


@cache
def taxon_metadata(taxon_id: str) -> dict:
    logger.info(f"Getting metadata for {taxon_id}")
    metadata = get_metadata(taxon_id)
    return {**metadata, "taxId": taxon_id}


