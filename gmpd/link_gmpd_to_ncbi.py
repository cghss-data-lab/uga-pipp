import ncbi
import pickle
import os


NOT_FOUND_FILE = "gmpd/species_not_found.txt"
searched_terms = set()
not_found_terms = set()

# Path to the pickle cache file
CACHE_FILE = "gmpd/gmpd_cache.pickle"

# Load the cache from the pickle file if it exists
cache = {}
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "rb") as f:
        cache = pickle.load(f)

# Function to save the cache to the pickle file
def save_cache():
    with open(CACHE_FILE, "wb") as f:
        pickle.dump(cache, f)

def write_to_not_found(term):
    global not_found_terms
    if term not in not_found_terms:
        with open(NOT_FOUND_FILE, "a") as f:
            f.write(term)
        not_found_terms.add(term)

def search_and_merge(term, SESSION):
    global cache, searched_terms, not_found_terms

    if term in cache:
        taxon = cache[term]
        ncbi.merge_taxon(taxon, SESSION)
        ncbi_id = taxon["taxId"]

    else:
        searched_terms.add(term)
        ncbi_id = ncbi.id_search(term)
        if ncbi_id:
            ncbi_metadata = ncbi.get_metadata(ncbi_id)
            taxon = {**ncbi_metadata, "taxId": ncbi_id}
            ncbi.merge_taxon(taxon, SESSION)
            cache[term] = taxon
            save_cache()
        else:
            not_found_terms.add(term)
            return None

    return ncbi_id

def link_gmpd_to_ncbi(row, SESSION):
    global searched_terms, not_found_terms
    host_species = row["HostReportedName"]
    pathogen_species = row["ParasiteReportedName"]
    
    try:
        host_ncbi_id = int(search_and_merge(host_species, SESSION))
        if host_ncbi_id:
            pathogen_ncbi_id = int(search_and_merge(pathogen_species, SESSION))
            if pathogen_ncbi_id:
                return (host_ncbi_id, pathogen_ncbi_id)
            if not pathogen_ncbi_id:
                return (host_ncbi_id, None)

        elif row["HostCorrectedName"]:
            host_ncbi_id = int(search_and_merge(row["HostCorrectedName"], SESSION))
            if host_ncbi_id:
                pathogen_ncbi_id = int(search_and_merge(pathogen_species, SESSION))
                if pathogen_ncbi_id:
                    return (host_ncbi_id, pathogen_ncbi_id)
        
        if not host_ncbi_id:
            write_to_not_found(f"No NCBI ID for host" + str({host_species}) + "or" + str({row['HostCorrectedName']}) +": \n")

        pathogen_ncbi_id = int(search_and_merge(pathogen_species, SESSION))
        if pathogen_ncbi_id:
            return (None, pathogen_ncbi_id)

        elif row["ParasiteCorrectedName"]:
            pathogen_ncbi_id = int(search_and_merge(row["ParasiteCorrectedName"], SESSION))
            if pathogen_ncbi_id:
                return (None, pathogen_ncbi_id)

        if not pathogen_ncbi_id:
            write_to_not_found(f"No NCBI ID for pathogen" + str({pathogen_species}) + "or" + str({row['ParasiteCorrectedName']}) +": \n")

        return (None, None)

    except Exception as e:
        write_to_not_found(f"Error getting taxon: {e}\n")
        return (None, None)

