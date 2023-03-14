import ncbi
import time

NOT_FOUND_FILE = "carnivoreGMPD/species_not_found.txt"
SLEEP_TIME = 0.5 #2 requests/second

def write_to_not_found(message):
    with open(NOT_FOUND_FILE, "a") as f:
        f.write(message)

def create_ncbi_taxon(host_species, pathogen_species, SESSION):
    looked_up = set()
    
    def lookup_taxon(ncbi_id):
        if ncbi_id and ncbi_id not in looked_up:
            looked_up.add(ncbi_id)
            taxon_metadata = ncbi.get_metadata(ncbi_id)
            taxon = {**taxon_metadata, "TaxId":ncbi_id}
            ncbi.merge_taxon(taxon, SESSION)
            time.sleep(SLEEP_TIME)
        
        elif not ncbi_id:
            write_to_not_found(f"No NCBI ID for species: {pathogen_species} \n")
    
    try:
        # search for the TaxID that matches the host_species name
        host_ncbi_id = ncbi.id_search(host_species)
        lookup_taxon(host_ncbi_id)

    except Exception as e:
        write_to_not_found(f"Error getting taxon for host {host_species}: {e} \n")

    try:
        pathogen_ncbi_id = ncbi.id_search(f"{pathogen_species}")
        lookup_taxon(pathogen_ncbi_id)

    except Exception as e:
        write_to_not_found(f"Error getting taxon for pathogen {pathogen_species}: {e}\n")


def link_host_pathogen(host_species, pathogen_species, SESSION):
    pairings_query = (
        "MATCH (t1:Taxon {name: $host_species}), (t2:Taxon {name: $pathogen_species}) "
        "MERGE (t2)-[:INFECTS]->(t1) "
    )
    SESSION.run(pairings_query, host_species=host_species, pathogen_species=pathogen_species)