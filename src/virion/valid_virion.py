import sys
import csv
from datetime import datetime
from loguru import logger


csv.field_size_limit(sys.maxsize)


def process_dates(year, month, day):
    try:
        date = datetime.strptime(
            f"{year}-{month}-{day}",
            "%Y-%m-%d",
        )
        return date.date()
    except ValueError:
        logger.trace("Incomplete date")
        return "null"


def process_accession(accession: str) -> list:
    if accession != "":
        ncbi_accession = accession.split(",")
        return ncbi_accession
    return []


def valid_virion(ncbiapi):
    ncbi_tax_ids = set()
    virion_valid = []
    with open("data/Virion.csv", "r", encoding="latin-1") as virion_file:
        virion = csv.DictReader(virion_file, delimiter="\t")

        for idx, row in enumerate(virion):
            if row["Database"] == "GLOBI":
                continue

            if row["HostTaxID"] == "" or row["VirusTaxID"] == "":
                continue

            row["report_id"] = idx

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

            ncbi_tax_ids.add(row["HostTaxID"])
            ncbi_tax_ids.add(row["VirusTaxID"])
            virion_valid.append(row)

    ncbi_hierarchies = [ncbiapi.search_hierarchy(ncbi_id) for ncbi_id in ncbi_tax_ids]

    return virion_valid, ncbi_tax_ids, ncbi_hierarchies
