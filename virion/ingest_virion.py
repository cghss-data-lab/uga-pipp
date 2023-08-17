from loguru import logger
from ncbi import merge_taxon
from virion.extract_properties import taxon_metadata, process_dates, process_metadata


def ingest_virion(session):
    with open("virion/data/Virion.csv", "r", encoding="utf-8") as virion:
        header = next(virion).strip().split("\t")

        for idx, line in enumerate(virion):
            row = line.split("\t")
            row[-1] = row[-1].strip()

            virion_dict = dict(zip(header, row))

            if virion_dict["HostTaxID"] == "":
                logger.warning(f"NCBI ID missing for host in row {idx}")
                continue

            if virion_dict["VirusTaxID"] == "":
                logger.warning(f"NCBI ID missing for pathogen in row {idx}")
                continue

            host_metadata = taxon_metadata(virion_dict["HostTaxID"])
            pathogen_metadata = taxon_metadata(virion_dict["VirusTaxID"])

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
            MERGE (r:Virion:Report {{ dataSource : "Virion",
                reportId : "Virion-{idx}",
                reportDate : "{report_date}",
                collectionDate : "{collection_date}"}})
            MERGE (h:Taxon {{ taxId: {virion_dict["HostTaxID"]} }})
            MERGE (p:Taxon {{ taxId : {virion_dict["VirusTaxID"]} }})
            MERGE (r)-[:ASSOCIATES {{role : "host"}} ]-(h)
            MERGE (r)-[:ASSOCIATES {{role : "pathogen",
                detectionType : "{virion_dict['DetectionMethod']}"}} ]-(p)
            """
            session.run(query)

            logger.info(f"Ingesting row: {idx}")
