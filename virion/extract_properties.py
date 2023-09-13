from datetime import datetime
from functools import cache
from loguru import logger
from ncbi.ncbi_api import NCBI


ncbi_api = NCBI()


def process_dates(year, month, day):
    try:
        date = datetime.strptime(
            f"{year}-{month}-{day}",
            "%Y-%m-%d",
        )
        return date.date()
    except ValueError:
        logger.warning("Incomplete date")
        return "null"


@cache
def taxon_metadata(taxon_id: str) -> dict:
    logger.info(f"Getting metadata for {taxon_id}")
    metadata = ncbi_api.get_metadata(taxon_id)
    return {**metadata, "taxId": taxon_id}


def process_accession(accession: str) -> list:
    if accession != "":
        ncbi_accession = accession.split(",")
        return ncbi_accession
    return []
