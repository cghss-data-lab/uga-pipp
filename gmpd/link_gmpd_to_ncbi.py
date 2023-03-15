import ncbi

NOT_FOUND_FILE = "gmpd/species_not_found.txt"

searched_terms = set()

def write_to_not_found(message):
    with open(NOT_FOUND_FILE, "a") as f:
        f.write(message)

def link_gmpd_to_ncbi(row, SESSION):
    global searched_terms
    host_species = row["HostReportedName"]
    pathogen_species = row["ParasiteReportedName"]
    
    try:
        # Search for host species
        if host_species not in searched_terms:
            searched_terms.add(host_species)
            host_ncbi_id = ncbi.id_search(host_species)

            if host_ncbi_id:
                host_ncbi_metadata = ncbi.get_metadata(host_ncbi_id)
                host_taxon = {**host_ncbi_metadata, "TaxId":host_ncbi_id}
                ncbi.merge_taxon(host_taxon, SESSION)
                return host_ncbi_id

        if row["HostCorrectedName"] and row["HostCorrectedName"] not in searched_terms:
            searched_terms.add(row["HostCorrectedName"])
            host_ncbi_id = ncbi.id_search(row["HostCorrectedName"])

            if host_ncbi_id:
                host_ncbi_metadata = ncbi.get_metadata(host_ncbi_id)
                host_taxon = {**host_ncbi_metadata, "TaxId":host_ncbi_id}
                ncbi.merge_taxon(host_taxon, SESSION)
                return host_ncbi_id

        write_to_not_found(f"No NCBI ID for host {host_species} or {row['HostCorrectedName']}: \n")

        # Search for pathogen species
        if pathogen_species not in searched_terms:
            searched_terms.add(pathogen_species)
            pathogen_ncbi_id = ncbi.id_search(pathogen_species)

            if pathogen_ncbi_id:
                pathogen_ncbi_metadata = ncbi.get_metadata(pathogen_ncbi_id)
                pathogen_taxon = {**pathogen_ncbi_metadata, "TaxId":pathogen_ncbi_id}
                ncbi.merge_taxon(pathogen_taxon, SESSION)
                return pathogen_ncbi_id

        if row["ParasiteCorrectedName"] and row["ParasiteCorrectedName"] not in searched_terms:
            searched_terms.add(row["ParasiteCorrectedName"])
            pathogen_ncbi_id = ncbi.id_search(row["ParasiteCorrectedName"])

            if pathogen_ncbi_id:
                pathogen_ncbi_metadata = ncbi.get_metadata(pathogen_ncbi_id)
                pathogen_taxon = {**pathogen_ncbi_metadata, "TaxId":pathogen_ncbi_id}
                ncbi.merge_taxon(pathogen_taxon, SESSION)
                return pathogen_ncbi_id

        write_to_not_found(f"No NCBI ID for pathogen {pathogen_species} or {row['ParasiteCorrectedName']} \n")
        return None

    except Exception as e:
        write_to_not_found(f"Error getting taxon: {e}\n")


