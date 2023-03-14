import ncbi

NOT_FOUND_FILE = "gmpd/species_not_found.txt"

searched_terms = set()

def write_to_not_found(message):
    with open(NOT_FOUND_FILE, "a") as f:
        f.write(message)

# Search for each of the host/path terms in GMPD
# If there's a term that matches in NCBI, create/merge taxon
def create_ncbi_taxon(host_species, pathogen_species, SESSION):
    global searched_terms
    try:
        if host_species not in searched_terms:
            # search for the TaxID that matches the host_reported name
            host_ncbi_id = ncbi.id_search(host_species)
            searched_terms.add(host_species)

            # if a term exists, then get the taxon metadata for that ID
            # merge the taxon on TaxID
            if host_ncbi_id:
                host_ncbi_metadata = ncbi.get_metadata(host_ncbi_id)
                host_taxon = {**host_ncbi_metadata, "TaxId":host_ncbi_id}
                ncbi.merge_taxon(host_taxon, SESSION)

            # elif host_ncbi_id still none
            # id_search for CorrectedName host_species_corrected

            else:
                write_to_not_found(f"No NCBI ID for host {host_species}: \n")

        if pathogen_species not in searched_terms:
            pathogen_ncbi_id = ncbi.id_search(f"{pathogen_species}")
            searched_terms.add(pathogen_species)

            if pathogen_ncbi_id:
                pathogen_ncbi_metadata = ncbi.get_metadata(pathogen_ncbi_id)
                pathogen_taxon = {**pathogen_ncbi_metadata, "TaxId": pathogen_ncbi_id}
                ncbi.merge_taxon(pathogen_taxon, SESSION)

            # elif pathogen_ncbi_id still none
            # id_search for CorrectedName pathogen_species_corrected

            else:
                write_to_not_found(f"No NCBI ID for pathogen {pathogen_species} \n")

    except Exception as e:
        write_to_not_found(f"Error getting taxon: {e}\n")


