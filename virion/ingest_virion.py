from loguru import logger
from ncbi.merge_taxon import merge_taxon
from virion.extract_properties import taxon_metadata, process_dates, process_accession


def ingest_virion(session):
    with open("virion/data/Virion.csv", "r", encoding="utf-8") as virion:
        header = next(virion).strip().split("\t")

        for idx, line in enumerate(virion):
            logger.info(f"Ingesting row: {idx}")
            row = line.split("\t")
            row[-1] = row[-1].strip()

            virion_dict = dict(zip(header, row))

            if virion_dict["Database"] == "GLOBI":
                continue

            if virion_dict["HostTaxID"] == "":
                logger.warning(f"NCBI ID missing for host in row {idx}")
                continue

            if virion_dict["VirusTaxID"] == "":
                logger.warning(f"NCBI ID missing for pathogen in row {idx}")
                continue

            host_metadata = taxon_metadata(virion_dict["HostTaxID"])
            pathogen_metadata = taxon_metadata(virion_dict["VirusTaxID"])

            ncbi_accession = process_accession(virion_dict["NCBIAccession"])

            collection_date = process_dates(
                virion_dict["CollectionYear"],
                virion_dict["CollectionMonth"],
                virion_dict["CollectionDay"],
            )

            report_date = process_dates(
                virion_dict["ReleaseYear"],
                virion_dict["ReleaseMonth"],
                virion_dict["ReleaseDay"],
            )

            merge_taxon(host_metadata, session)
            merge_taxon(pathogen_metadata, session)

            query = f"""
            CREATE (r:Virion:Report {{ dataSource : "Virion",
                reportId : "Virion-{idx}",
                reportDate : "{report_date}",
                collectionDate : "{collection_date}",
                ncbiAccession : {ncbi_accession} }})
            MERGE (h:Taxon {{ taxId: {virion_dict["HostTaxID"]} }})
            MERGE (p:Taxon {{ taxId : {virion_dict["VirusTaxID"]} }})
            MERGE (r)-[:ASSOCIATES {{role : "host"}} ]-(h)
            MERGE (r)-[:ASSOCIATES {{role : "pathogen",
                detectionType : "{virion_dict['DetectionMethod']}"}} ]-(p)
            """
            session.run(query)
