import ncbi
import time

NOT_FOUND_FILE = "not_found.txt"
SLEEP_TIME = 0.5 #2 requests/second

def write_to_not_found(message):
    with open(NOT_FOUND_FILE, "a") as f:
        f.write(message)

def create_ncbi_taxon(host_species, pathogen_species, SESSION):
    try:
        # search for the TaxID that matches the host_species name
        host_ncbi_id = ncbi.id_search(host_species)

        # if a term exists, then get the taxon metadata for that ID
        # merge the taxon on TaxID
        if host_ncbi_id:
            host_ncbi_metadata = ncbi.get_metadata(host_ncbi_id)
            host_taxon = {**host_ncbi_metadata, "TaxId":host_ncbi_id}
            ncbi.merge_taxon(host_taxon, SESSION)
            time.sleep(SLEEP_TIME)
        
        else:
            write_to_not_found(f"No NCBI ID for host {host_species}: \n")


    except Exception as e:
        write_to_not_found(f"Error getting taxon for host {host_species}: {e} \n")

    try:
        pathogen_ncbi_id = ncbi.id_search(f"{pathogen_species}")
        if pathogen_ncbi_id:
            pathogen_ncbi_metadata = ncbi.get_metadata(pathogen_ncbi_id)
            pathogen_taxon = {**pathogen_ncbi_metadata, "TaxId": pathogen_ncbi_id}
            ncbi.merge_taxon(pathogen_taxon, SESSION)
            time.sleep(SLEEP_TIME)

        else:
            write_to_not_found(f"No NCBI ID for pathogen {pathogen_species} \n")

    except Exception as e:
        write_to_not_found(f"Error getting taxon for pathogen {pathogen_species}: {e}\n")    


def label_ncbi_taxon(host_species, pathogen_species, SESSION):
    host_query = (
        "MERGE (t:Taxon {name: $host_species})"
        "SET t:Host"
    )
    SESSION.run(host_query, host_species=host_species)

    pathogen_query = (
        "MERGE (t:Taxon {name: $pathogen_species})"
        "SET t:Pathogen"
    )
    SESSION.run(pathogen_query, pathogen_species=pathogen_species)

def link_host_pathogen(host_species, pathogen_species, SESSION):
    pairings_query = (
        "MATCH (h:Host {name: $host_species}), (p:Pathogen {name: $pathogen_species}) "
        "MERGE (p)-[:INFECTS]->(h) "
    )
    SESSION.run(pairings_query, host_species=host_species, pathogen_species=pathogen_species)