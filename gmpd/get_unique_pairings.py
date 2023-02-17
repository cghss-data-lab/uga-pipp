import gmpd

def get_unique_pairings():
    rows = gmpd.get_rows()

    hostpath_pairings = set()
    for row in rows:
        hostpath_pairings.add(row["species"] + "|" + row["ParasiteGMPD"])
    
    return hostpath_pairings