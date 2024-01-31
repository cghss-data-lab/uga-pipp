import csv


def is_float(number: str) -> bool:
    try:
        float(number)
    except ValueError:
        return False
    return True


def is_valid_report(row: dict) -> bool:
    if not is_float(row["Prevalence"]) or not is_float(row["NumSamples"]):
        return False
    if row["Latitude"] == "NA" or row["Longitude"] == "NA":
        return False
    return True


def valid_gmpd(geoapi, ncbi_api, file: str = "data/GMPD_main.csv") -> list[dict]:
    geonames = set()
    tax_names = set()
    gmpd_valid = []

    with open(file, "r", encoding="utf-8-sig") as gmpd_file:
        gmpd = csv.DictReader(gmpd_file)
        for idx, row in gmpd:
            if not is_valid_report(row):
                continue

            row["Positive"] = float(row["Prevalence"]) * float(row["NumSamples"])
            locations = (row["Latitude"], row["Longitude"])
            row["reportId"] = idx


            geonames.add(locations)
            tax_names.add(row["HostCorrectedName"])
            tax_names.add(row["ParasiteCorrectedName"])

            gmpd_valid.append(row)

        geonames_id = [geoapi.search_lat_long(territory) for territory in geonames]
        tax_id = [ncbi_api.search_id(tax) for tax in tax_names]

    return gmpd_valid, geonames, geonames_id, tax_names, tax_id
