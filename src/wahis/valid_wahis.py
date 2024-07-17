from datetime import datetime
from src.wahis.wahis_api import WAHISApi
from network.handle_concurrency import handle_concurrency


def process_dates(date: str) -> str:
    if not date:
        return None

    date_strip = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f%z")
    return date_strip.strftime("%Y-%m-%d")


def remove_unneeded_keys(row: dict) -> None:
    row.pop("sources", None)
    row.pop("measures", None)
    row.pop("methods", None)
    row.pop("strategy", None)
    row.pop("laboratoryTests", None)
    row.pop("selfDeclaration", None)


def process_report(metadata: dict, tax_names: set, lat_long: set):
    metadata["report"]["reportId"] = metadata['report']['reportId']
    metadata["report"]["reportedOn"] = process_dates(metadata["report"]["reportedOn"])
    metadata['event']['confirmOn'] = process_dates(metadata['event']['confirmOn'])

    if metadata["event"]["eventComment"]:
        metadata["event"]["eventComment"] = (
            metadata["event"]["eventComment"]["translation"]
            or metadata["event"]["eventComment"]["original"]
        )

    species_quantities = metadata['quantitativeData']['news']
    species_totals = metadata['quantitativeData']['totals']
    if species_quantities:
        for species in species_quantities:
            host_search_name = species['speciesName']
            tax_names.add(host_search_name)

            
    elif species_totals:
        for species in species_totals:
            host_search_name = species['speciesName']
            tax_names.add(host_search_name)
    else:
        host_search_name = metadata['event']['disease']['group']
        tax_names.add(host_search_name)

    subtype = metadata["event"]["subType"]
    path_search_name = None
    if subtype and 'disease' in subtype:
        sero = subtype['disease']
        if sero and 'name' in sero:
            path_search_name = sero['name']
            tax_names.add(path_search_name)

    if not path_search_name:
        path = metadata['event']['causalAgent']
        if path and 'name' in path:
            path_search_name = path['name']
            tax_names.add(path_search_name)

    if not path_search_name:
        path = metadata['event']['disease']
        if path and 'name' in path:
            path_search_name = path['name']
            tax_names.add(path_search_name)

    for outbreak in metadata["outbreaks"]:
        location = (outbreak["latitude"], outbreak["longitude"])
        outbreak["geonames"] = location
        lat_long.add(location)

        outbreak["start_date"] = process_dates(outbreak["startDate"])
        outbreak["end_date"] = process_dates(outbreak["endDate"])

    remove_unneeded_keys(metadata)
    return metadata


def pivot_long(wahis: list) -> list:
    new_wahis = []
    for row in wahis:
        outbreaks = row["outbreaks"]
        row.pop("outbreaks")
        for out in outbreaks:
            new_row = row.copy()
            new_row["outbreak"] = out
            new_wahis.append(new_row)

    return new_wahis


def is_valid(row: dict, empty: tuple = (None, "")) -> bool:
    if row["event"]["disease"]["group"] in empty:
        return False

    if row["event"]["disease"]["name"] in empty:
        return False

    return True


async def valid_wahis(geoapi, ncbiapi, wahis=WAHISApi()) -> list:
    lat_long = set()
    tax_names = set()

# Set the range for the WAHIS events you'd like to ingest. 1 is the first report
# The last report varies (always increasing), but as of 7/10 was 5729

    evolutions = await handle_concurrency(
        *[wahis.search_evolution(event_id) for event_id in range(5100, 5400)]
    )

    reports = await handle_concurrency(
        *[
            wahis.search_report(report["reportId"])
            for evolution in evolutions
            if evolution is not None
            for report in evolution
        ]
    )

    wahis_valid = [process_report(report, tax_names, lat_long) for report in reports]

    geoname_ids = await handle_concurrency(
        *[geoapi.search_lat_long(location) for location in lat_long]
    )

    tax_ids = await handle_concurrency(
        *[ncbiapi.search_id(tax) for tax in tax_names], n_semaphore=2
    )
    geonames = dict(zip(lat_long, geoname_ids))

    wahis_valid = [x for x in wahis_valid if is_valid(x)]
    wahis_valid = pivot_long(wahis_valid)

    return wahis_valid, geonames, tax_names, tax_ids
