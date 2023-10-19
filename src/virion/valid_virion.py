import csv
from datetime import datetime
from loguru import logger


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


def process_accession(accession: str) -> list:
    if accession != "":
        ncbi_accession = accession.split(",")
        return ncbi_accession
    return []


def valid_virion(ncbiapi):
    ncbi_tax_ids = set()
    virion_valid = []
    with open("virion/data/Virion.csv", "r", encoding="utf-8") as virion_file:
        logger.info("Validating Virion")
        virion = csv.DictReader(virion_file)

        for idx, row in enumerate(virion):
            if row["Database"] == "GLOBI":
                continue

            if row["HostTaxID"] == "" or row["VirusTaxID"] == "":
                continue

            row["reportId"] = f"Virion-{idx}"

            row["ncbi_accession"] = process_accession(row["NCBIAccession"])

            row["collection_date"] = process_dates(
                row["CollectionYear"],
                row["CollectionMonth"],
                row["CollectionDay"],
            )

            row["report_date"] = process_dates(
                row["ReleaseYear"],
                row["ReleaseMonth"],
                row["ReleaseDay"],
            )

            ncbi_tax_ids.add(row["VirusTaxID"])
            ncbi_tax_ids.add(row["VirusTaxID"])
            virion_valid.append(row)

    ncbi_hierarchies = [ncbiapi.search_hierarchy(ncbi_id) for ncbi_id in ncbi_tax_ids]

    return virion_valid, ncbi_hierarchies
