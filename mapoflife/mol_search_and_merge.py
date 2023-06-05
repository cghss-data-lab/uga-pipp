import os
import pickle
import ncbi

MOL_NOT_FOUND = "mol/species_not_found.txt"
mol_searched = set()
mol_not_found = set()

# Path to the pickle cache file
MOL_CACHE = "mol/mol_cache.pickle"

# Load the cache from the pickle file if it exists
mol_cache = {}
if os.path.exists(MOL_CACHE):
    with open(MOL_CACHE, "rb") as f:
        mol_cache = pickle.load(f)

# Function to save the cache to the pickle file
def save_cache():
    with open(MOL_CACHE, "wb") as f:
        pickle.dump(mol_cache, f)

def write_to_not_found(term):
    global mol_not_found
    if term not in mol_not_found:
        with open(MOL_NOT_FOUND, "a") as f:
            f.write(term)
        mol_not_found.add(term)

def mol_search_and_merge(term, SESSION):
    global mol_cache, mol_searched, mol_not_found

    if term in mol_cache:
        taxon = mol_cache[term]
        ncbi.merge_taxon(taxon, SESSION)
        ncbi_id = taxon["taxId"]

    else:
        mol_searched.add(term)
        ncbi_id = ncbi.id_search(term)
        if ncbi_id:
            ncbi_metadata = ncbi.get_metadata(ncbi_id)
            taxon = {**ncbi_metadata, "taxId": ncbi_id}
            ncbi.merge_taxon(taxon, SESSION)
            mol_cache[term] = taxon
            save_cache()
        else:
            mol_not_found.add(term)
            return None

    return ncbi_id