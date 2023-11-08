import csv
from datetime import datetime


FIELDNAMES = (
    "reportId",
    "Territory",
    "WHO region",
    "Transmission zone",
    "Year",
    "Week",
    "startDate",
    "endDate",
    "Collected",
    "Processed",
    "A (H1)",
    "A (H1N1)",
    "A (H3)",
    "A (H5)",
    "A (not subtyped)",
    "A (total)",
    "B (Yamagata)",
    "B (Victoria)",
    "B (not subtyped)",
    "B (total)",
    "caseCount",
    "Total negative",
    "ILI activity",
)


def is_valid_report(data: dict) -> bool:
    counts = [
        data["Collected"],
        data["Processed"],
        data["caseCount"],
        data["Total negative"],
    ]
    counts = [x in ["", "0"] for x in counts]

    if all(counts):
        return False
    return True


def process_dates(data: dict) -> None:
    data["startDate"] = datetime.strptime(data["startDate"], "%m/%d/%y").strftime(
        "%Y-%m-%d"
    )
    data["endDate"] = datetime.strptime(data["endDate"], "%m/%d/%y").strftime(
        "%Y-%m-%d"
    )


def split_influenza_type(valid: list[dict]) -> tuple[list[dict]]:
    influenza_a = [data for data in valid if data["A (total)"] not in ["", "0"]]
    influenza_a = [
        dict(data, **{"type": 11320, "name": "Influenza A virus"})
        for data in influenza_a
    ]
    influenza_b = [data for data in valid if data["B (total)"] not in ["", "0"]]
    influenza_b = [
        dict(data, **{"type": 11520, "name": "Influenza B virus"})
        for data in influenza_b
    ]
    return influenza_a, influenza_b


def valid_flunet(geo_api, file: str = "data/flunet_1995_2022.csv") -> list[dict]:
    flunet_valid = list()
    geonames = set()
    geoname_ids = []

    with open(file, "r", encoding="utf-8") as flunet_file:
        flunet = csv.DictReader(flunet_file)
        flunet.fieldnames = FIELDNAMES
        next(flunet)
        for row in flunet:
            if not is_valid_report(row):
                continue

            process_dates(row)

            row["reportId"] = f"FluNet-{row['reportId']}"
            row["eventId"] = f"Flu-{row['Territory']}-{str(row['startDate'])}"

            flunet_valid.append(row)

            # Process territories
            row["Territory"] = territory = (
                row["Territory"].lower().strip().replace('"', "")
            )
            if territory not in geonames and territory is not None:
                geoname_ids.append(geo_api.search_geoname_id(territory))

            geonames.add(territory)

    return flunet_valid, geonames, geoname_ids
