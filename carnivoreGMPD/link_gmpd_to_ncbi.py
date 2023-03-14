import ncbi

NOT_FOUND_FILE = "carnivoreGMPD/species_not_found.txt"

searched_terms = set() #keep track of terms that have been searched for

def write_to_not_found(message):
    with open(NOT_FOUND_FILE, "a") as f:
        f.write(message)

def create_ncbi_taxon(host_species, pathogen_species, SESSION):
    global searched_terms
    try:
        if host_species not in searched_terms:
            # search for the TaxID that matches the host_species name
            host_ncbi_id = ncbi.id_search(host_species)
            searched_terms.add(host_species)

            # if a term exists, then get the taxon metadata for that ID
            # merge the taxon on TaxID
            if host_ncbi_id:
                host_ncbi_metadata = ncbi.get_metadata(host_ncbi_id)
                host_taxon = {**host_ncbi_metadata, "TaxId":host_ncbi_id}
                ncbi.merge_taxon(host_taxon, SESSION)
        
            else:
                write_to_not_found(f"No NCBI ID for host {host_species}: \n")

        if pathogen_species not in searched_terms:
            pathogen_ncbi_id = ncbi.id_search(f"{pathogen_species}")
            searched_terms.add(pathogen_species)

            if pathogen_ncbi_id:
                pathogen_ncbi_metadata = ncbi.get_metadata(pathogen_ncbi_id)
                pathogen_taxon = {**pathogen_ncbi_metadata, "TaxId": pathogen_ncbi_id}
                ncbi.merge_taxon(pathogen_taxon, SESSION)

            else:
                write_to_not_found(f"No NCBI ID for pathogen {pathogen_species} \n")

    except Exception as e:
        write_to_not_found(f"Error getting taxon: {e}\n")

def link_host_pathogen(host_species, pathogen_species, SESSION):
    pairings_query = (
        "MATCH (t1:Taxon {name: $host_species}), (t2:Taxon {name: $pathogen_species}) "
        "MERGE (t2)-[:CARRIES]->(t1) "
    )
    SESSION.run(pairings_query, host_species=host_species, pathogen_species=pathogen_species)
