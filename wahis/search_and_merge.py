import pickle
import os
from ncbi.merge_taxon import merge_taxon
from ncbi.ncbi_api import NCBI

WAHIS_NOT_FOUND = "wahis/species_not_found.txt"
wahis_searched = set()
wahis_not_found = set()

# Path to the pickle cache file
WAHIS_CACHE = "wahis/wahis_cache.pickle"

ncbi_api = NCBI()

# Load the cache from the pickle file if it exists
wahis_cache = {}
if os.path.exists(WAHIS_CACHE):
    with open(WAHIS_CACHE, "rb") as f:
        wahis_cache = pickle.load(f)


# Function to save the cache to the pickle file
def save_cache():
    with open(WAHIS_CACHE, "wb") as f:
        pickle.dump(wahis_cache, f)


def write_to_not_found(term):
    global wahis_not_found
    if term not in wahis_not_found:
        with open(WAHIS_NOT_FOUND, "a", encoding="utf-8") as f:
            f.write(term)
        wahis_not_found.add(term)


def search_and_merge(term, SESSION):
    global wahis_cache, wahis_searched, wahis_not_found

    if term in wahis_cache:
        taxon = wahis_cache[term]
        merge_taxon(taxon, SESSION)
        ncbi_id = taxon["taxId"]

    elif term in wahis_not_found:
        return None

    else:
        wahis_searched.add(term)
        ncbi_id = ncbi_api.id_search(term)
        if ncbi_id:
            ncbi_metadata = ncbi_api.get_metadata(ncbi_id)
            taxon = {**ncbi_metadata, "taxId": ncbi_id}
            merge_taxon(taxon, SESSION)
            wahis_cache[term] = taxon
            save_cache()
        else:
            wahis_not_found.add(term)
            return None

    return ncbi_id
