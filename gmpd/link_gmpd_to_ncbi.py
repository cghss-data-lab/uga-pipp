import ncbi

NOT_FOUND_FILE = "gmpd/species_not_found.txt"

searched_terms = set()

def write_to_not_found(message):
    with open(NOT_FOUND_FILE, "a") as f:
        f.write(message)

def link_gmpd_to_ncbi(row, SESSION):
    global searched_terms
    try:
        host_species = row["HostReportedName"]
        pathogen_species = row["ParasiteReportedName"]
        
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

            elif row["HostCorrectedName"] not in searched_terms:
                # if the reported name doesn't match, try the corrected name
                host_ncbi_id = ncbi.id_search(row["HostCorrectedName"])
                searched_terms.add(row["HostCorrectedName"])

                if host_ncbi_id:
                    host_ncbi_metadata = ncbi.get_metadata(host_ncbi_id)
                    host_taxon = {**host_ncbi_metadata, "TaxId":host_ncbi_id}
                    ncbi.merge_taxon(host_taxon, SESSION)

                else:
                    write_to_not_found(f"No NCBI ID for host {host_species} or {row['HostCorrectedName']}: \n")

            else:
                pass

        if pathogen_species not in searched_terms:
            pathogen_ncbi_id = ncbi.id_search(pathogen_species)
            searched_terms.add(pathogen_species)

            if pathogen_ncbi_id:
                pathogen_ncbi_metadata = ncbi.get_metadata(pathogen_ncbi_id)
                pathogen_taxon = {**pathogen_ncbi_metadata, "TaxId": pathogen_ncbi_id}
                ncbi.merge_taxon(pathogen_taxon, SESSION)

            elif row["ParasiteCorrectedName"] not in searched_terms:
                pathogen_ncbi_id = ncbi.id_search(row["ParasiteCorrectedName"])
                searched_terms.add(row["ParasiteCorrectedName"])

                if pathogen_ncbi_id:
                    pathogen_ncbi_metadata = ncbi.get_metadata(pathogen_ncbi_id)
                    pathogen_taxon = {**pathogen_ncbi_metadata, "TaxId": pathogen_ncbi_id}
                    ncbi.merge_taxon(pathogen_taxon, SESSION)

                else:
                    write_to_not_found(f"No NCBI ID for pathogen {pathogen_species} or {row['ParasiteCorrectedName']} \n")

            else:
                pass

        # Return the host and pathogen tax IDs
        host_tax_id = host_ncbi_id
        pathogen_tax_id = pathogen_ncbi_id
        return (host_tax_id, pathogen_tax_id)

    except Exception as e:
        write_to_not_found(f"Error getting taxon: {e}\n")


