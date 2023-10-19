import csv
from datetime import datetime
from loguru import logger


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


def valid_virion():
    virion_valid = []
    with open("virion/data/Virion.csv", "r", encoding="utf-8") as virion_file:
        virion = csv.DictReader(virion_file)

        for idx, row in enumerate(virion):
            logger.info(f"Ingesting row: {idx}")

            virion_valid.append(row)
