import carnivoreGMPD
import carnivoreGMPD.link_gmpd_to_ncbi as link_gmpd_to_ncbi

def ingest_gmpd(SESSION):

    tax_list = set()
    gmpd_rows = carnivoreGMPD.get_rows()

    for row in gmpd_rows:

        host_species = row['host_species']
        pathogen_species = row['pathogen_species']

        # Create taxa from NCBI
        link_gmpd_to_ncbi.create_ncbi_taxon(host_species, pathogen_species, SESSION)

        # # Label the NCBI taxon nodes
        # # Commented out because host / pathogen status is dependent on the interaction between species (not the taxon itself)
        # # To re-add, uncomment below and in link_ncbi_gmpd.py

        # link_ncbi_gmpd.label_ncbi_taxon(host_species, pathogen_species, SESSION)

        # Create pairings
        link_gmpd_to_ncbi.link_host_pathogen(host_species, pathogen_species, SESSION)