from datetime import datetime
from src.wahis.wahis_api import WAHISApi
from network.handle_concurrency import handle_concurrency


def process_dates(date: str) -> str:
    date_strip = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f%z")
    return date_strip.strftime("%Y-%m-%d")


async def valid_wahis(geoapi, ncbiapi, wahis=WAHISApi()) -> list:
    wahis_valid = []
    lat_long = set()

    for event_id in range(4714, 5097):
        evolution = await wahis.search_evolution(event_id)

        if not evolution:
            continue

        for report in evolution:
            report_id = report["reportId"]

            metadata = await wahis.search_report(report_id)

            metadata["dataSource"] = "WAHIS"
            metadata["report"]["uqReportId"] = "WAHIS-" + str(report_id)
            metadata["report"]["reportedOn"] = process_dates(
                metadata["reported"]["reportedOn"]
            )

            if metadata["event"]["eventComment"]:
                metadata["event"]["eventComment"] = (
                    metadata["event"]["eventComment"]["translation"]
                    or metadata["event"]["eventComment"]["original"]
                )

            for outbreak in metadata["outbreaks"]:
                lat_long.add((outbreak["latitude"], outbreak["longitude"]))
                outbreak["startDate"] = process_dates(outbreak["startDate"])
                outbreak["endDate"] = process_dates(outbreak["endDate"])

            wahis_valid.append(metadata)

    lat_long = handle_concurrency(
        *[await geoapi.search_lat_long(location) for location in lat_long]
    )

    return wahis_valid, lat_long
