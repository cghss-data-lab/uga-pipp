import ncbi

NOT_FOUND_FILE = "gmpd/species_not_found.txt"

searched_terms = set()
not_found_terms = set()

def write_to_not_found(message):
    global not_found_terms
    if message not in not_found_terms:
        with open(NOT_FOUND_FILE, "a") as f:
            f.write(message)
        not_found_terms.add(message)

def search_and_merge(term, SESSION):
    global searched_terms
    if term not in searched_terms:
        searched_terms.add(term)
        ncbi_id = ncbi.id_search(term)
        if ncbi_id:
            ncbi_metadata = ncbi.get_metadata(ncbi_id)
            taxon = {**ncbi_metadata, "TaxId":ncbi_id}
            ncbi.merge_taxon(taxon, SESSION)
            return ncbi_id
    return None

def link_gmpd_to_ncbi(row, SESSION):
    host_species = row["HostReportedName"]
    pathogen_species = row["ParasiteReportedName"]
    
    try:
        host_ncbi_id = search_and_merge(host_species, SESSION)
        if host_ncbi_id:
            pathogen_ncbi_id = search_and_merge(pathogen_species, SESSION)
            if pathogen_ncbi_id:
                return (host_ncbi_id, pathogen_ncbi_id)

        elif row["HostCorrectedName"]:
            host_ncbi_id = search_and_merge(row["HostCorrectedName"], SESSION)
            if host_ncbi_id:
                pathogen_ncbi_id = search_and_merge(pathogen_species, SESSION)
                if pathogen_ncbi_id:
                    return (host_ncbi_id, pathogen_ncbi_id)
        
        if not host_ncbi_id:
            write_to_not_found(f"No NCBI ID for host {host_species} or {row['HostCorrectedName']}: \n")

        pathogen_ncbi_id = search_and_merge(pathogen_species, SESSION)
        if pathogen_ncbi_id:
            return (None, pathogen_ncbi_id)

        elif row["ParasiteCorrectedName"]:
            pathogen_ncbi_id = search_and_merge(row["ParasiteCorrectedName"], SESSION)
            if pathogen_ncbi_id:
                return (None, pathogen_ncbi_id)

        if not pathogen_ncbi_id:
            write_to_not_found(f"No NCBI ID for pathogen {pathogen_species} or {row['ParasiteCorrectedName']} \n")

        return (None, None)

    except Exception as e:
        write_to_not_found(f"Error getting taxon: {e}\n")
        return (None, None)
