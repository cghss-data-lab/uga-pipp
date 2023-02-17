import re
import gmpd

def get_unique_taxons():
    rows = gmpd.get_rows()
    gmpd_hosts = set()
    gmpd_pathogens = set()
    for row in rows:
        gmpd_hosts.add(row['species'].strip())
        gmpd_pathogens.add(row['ParasiteGMPD'].strip())
    return gmpd_hosts, gmpd_pathogens
