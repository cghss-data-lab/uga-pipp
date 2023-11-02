from datetime import datetime
from src.wahis.wahis_api import WAHISApi
from network.handle_concurrency import handle_concurrency


def process_dates(date: str) -> str:
    if not date:
        return None

    date_strip = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f%z")
    return date_strip.strftime("%Y-%m-%d")


async def valid_wahis(geoapi, ncbiapi, wahis=WAHISApi()) -> list:
    wahis_valid = []
    lat_long = set()
    tax_names = set()

    for event_id in range(4714, 4715):  # (4714, 5097):
        evolution = await wahis.search_evolution(event_id)

        if not evolution:
            continue

        for report in evolution:
            report_id = report["reportId"]

            metadata = await wahis.search_report(report_id)

            metadata["report"]["uqReportId"] = f"WAHIS-{str(report_id)}"
            metadata["report"]["reportedOn"] = process_dates(
                metadata["report"]["reportedOn"]
            )

            if metadata["event"]["eventComment"]:
                metadata["event"]["eventComment"] = (
                    metadata["event"]["eventComment"]["translation"]
                    or metadata["event"]["eventComment"]["original"]
                )

            tax_names.add(metadata["event"]["disease"]["group"])
            tax_names.add(metadata["event"]["causalAgent"]["name"])

            metadata["quantitativeData"]["totals"] = metadata["quantitativeData"][
                "totals"
            ][0]

            for outbreak in metadata["outbreaks"]:
                location = (outbreak["latitude"], outbreak["longitude"])
                outbreak["geonames"] = location
                lat_long.add(location)

                outbreak["startDate"] = process_dates(outbreak["startDate"])
                outbreak["endDate"] = process_dates(outbreak["endDate"])

            wahis_valid.append(metadata)

    geoname_ids = await handle_concurrency(
        *[geoapi.search_lat_long(location) for location in lat_long]
    )

    tax_ids = await handle_concurrency(*[ncbiapi.search_id(tax) for tax in tax_names])
    geonames = dict(zip(lat_long, geoname_ids))

    return wahis_valid, geonames, tax_names, tax_ids
