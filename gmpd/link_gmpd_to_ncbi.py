import ncbi

NOT_FOUND_FILE = "gmpd/species_not_found.txt"


def write_to_not_found(message):
    not_found_terms = set()
    if message not in not_found_terms:
        with open(NOT_FOUND_FILE, "a") as f:
            f.write(message)
        not_found_terms.add(message)

def search_and_merge(term, searched_terms, not_found_terms, SESSION):
    if term in searched_terms and term in not_found_terms:
        return None

    searched_terms.add(term)
    ncbi_id = ncbi.id_search(term)
    if ncbi_id:
        ncbi_metadata = ncbi.get_metadata(ncbi_id)
        taxon = {**ncbi_metadata, "TaxId":ncbi_id}
        ncbi.merge_taxon(taxon, SESSION)
        return ncbi_id
    else:
        not_found_terms.add(term)
        return None


def link_gmpd_to_ncbi(row, searched_terms, not_found_terms, SESSION):
    host_species = row["HostReportedName"]
    pathogen_species = row["ParasiteReportedName"]
    
    try:
        host_ncbi_id = int(search_and_merge(host_species, searched_terms, not_found_terms, SESSION))
        if host_ncbi_id:
            pathogen_ncbi_id = int(search_and_merge(pathogen_species, searched_terms, not_found_terms, SESSION))
            if pathogen_ncbi_id:
                return (host_ncbi_id, pathogen_ncbi_id)

        elif row["HostCorrectedName"]:
            host_ncbi_id = int(search_and_merge(row["HostCorrectedName"], searched_terms, not_found_terms, SESSION))
            if host_ncbi_id:
                pathogen_ncbi_id = int(search_and_merge(pathogen_species, searched_terms, not_found_terms, SESSION))
                if pathogen_ncbi_id:
                    return (host_ncbi_id, pathogen_ncbi_id)
        
        if not host_ncbi_id:
            write_to_not_found(f"No NCBI ID for host {host_species} or {row['HostCorrectedName']}: \n")

        pathogen_ncbi_id = int(search_and_merge(pathogen_species, searched_terms, not_found_terms, SESSION))
        if pathogen_ncbi_id:
            return (None, pathogen_ncbi_id)

        elif row["ParasiteCorrectedName"]:
            pathogen_ncbi_id = int(search_and_merge(row["ParasiteCorrectedName"], searched_terms, not_found_terms, SESSION))
            if pathogen_ncbi_id:
                return (None, pathogen_ncbi_id)

        if not pathogen_ncbi_id:
            write_to_not_found(f"No NCBI ID for pathogen {pathogen_species} or {row['ParasiteCorrectedName']} \n")

        return (None, None)

    except Exception as e:
        write_to_not_found(f"Error getting taxon: {e}\n")
        return (None, None)

