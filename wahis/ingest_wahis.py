import time
from datetime import datetime
from loguru import logger
import requests
from geonames.geo_api import GeonamesApi
from geonames import merge_geo
import wahis
from wahis import search_and_merge


search_lat_long = GeonamesApi().search_lat_long


def ingest_wahis(SESSION):
    for i in range(4714, 5097):  # events as of 7/18/23
        try:
            listId = i
            evolution_list = wahis.get_evolution(listId)

            # Check if the evolution_list is empty or None
            if not evolution_list:
                logger.info(f"No evolution list found for listId {listId}. Skipping...")
                continue

            # for each report in the evolution list of reports, create a report
            for x in range(len(evolution_list)):
                reportId = evolution_list[x]
                uqReportId = "WAHIS-" + str(reportId)

                logger.info(f"GETTING report id {reportId}")
                metadata = wahis.get_report(reportId)

                if "event" not in metadata:
                    logger.error(f"Report {reportId} does not exist. Skipping...")
                    continue

                # Sections to make it easier
                event = metadata["event"]
                report = metadata["report"]
                outbreaks = metadata["outbreaks"]

                # On the report node
                reported_str = report["reportedOn"]
                reported_strip = datetime.strptime(
                    reported_str, "%Y-%m-%dT%H:%M:%S.%f%z"
                )
                reported = reported_strip.strftime("%Y-%m-%d")
                reasonForNotification = event["reason"]["translation"]
                eventComment = event["eventComment"]
                if eventComment and eventComment["translation"] is not None:
                    eventDescription = eventComment["translation"]
                elif eventComment and eventComment["original"] is not None:
                    eventDescription = eventComment["original"]
                else:
                    eventDescription = "NA"

                iso3 = event["country"]["isoCode"]

                # MERGE a REPORT node
                report_params = {
                    "dataSource": "WAHIS",
                    "uqReportId": uqReportId,
                    "reportDate": reported,
                    "reasonForReport": reasonForNotification,
                    "description": eventDescription,
                }

                report_query = """
                    MERGE (r:Report:WAHIS {
                        dataSource: $dataSource,
                        uqReportId: $uqReportId,
                        reportDate: $reportDate,
                        reasonForReport: $reasonForReport,
                        description: $description
                    })
                    RETURN r
                """

                # RUN query to create REPORT node
                logger.info(f"MERGE report ID: {reportId}")
                report_result = SESSION.run(report_query, report_params)
                report_node = report_result.single()[0]

                # Set to track processed eventIds
                processed_event_ids = set()

                # For each outbreak event listed in the report, grab metadata
                for index, key in enumerate(outbreaks):
                    try:
                        if i == 3721:
                            if index < 596:
                                continue

                        # EVENT :OCCURS_IN GEO
                        place = key["location"]
                        long = key["longitude"]
                        lat = key["latitude"]
                        if "description" in key and key["description"] is not None:
                            desc = key["description"]
                        else:
                            desc = "NA"

                        # Return the location geonameId if there is one
                        if lat and long:
                            geonameId = search_lat_long((lat, long))
                            merge_geo(geonameId, SESSION)
                        elif place:
                            merge_geo(geonameId, SESSION)
                        else:
                            geonameId = None  # TODO: Use iso3

                        eventId = key["outbreakId"]

                        # Check if the eventId has already been processed and skip processing duplicates
                        if eventId in processed_event_ids:
                            continue

                        # Add eventId to set of processed events
                        processed_event_ids.add(eventId)

                        outbreak_metadata = wahis.get_outbreak(reportId, eventId)

                        if outbreak_metadata is None:
                            continue

                        outbreak_str = outbreak_metadata["outbreak"]["startDate"]
                        start_strip = datetime.strptime(
                            outbreak_str, "%Y-%m-%dT%H:%M:%S.%f%z"
                        )
                        outbreakStart = start_strip.strftime("%Y-%m-%d")

                        outbreak_end = outbreak_metadata["outbreak"]["endDate"]
                        if outbreak_end:
                            end_strip = datetime.strptime(
                                outbreak_end, "%Y-%m-%dT%H:%M:%S.%f%z"
                            )
                            outbreakEnd = end_strip.strftime("%Y-%m-%d")
                        else:
                            outbreakEnd = "Ongoing"

                        if outbreakEnd != "Ongoing":
                            dur = end_strip - start_strip
                            duration = f"P{dur.days}D"
                        else:
                            duration = "NA"

                        # MERGE Report and Event nodes so that events with same eventId but different reportIds are merged
                        event_query = """
                        MERGE (e:Event:Outbreak {
                            eventId: $eventId,
                            startDate: $outbreakStart,
                            endDate: $outbreakEnd,
                            description: $description,
                            duration: $duration
                        })
                        WITH e
                        MATCH (r:Report:WAHIS {uqReportId: $uqReportId})
                        MERGE (r)-[:REPORTS]->(e)
                        """

                        event_params = {
                            "uqReportId": uqReportId,
                            "eventId": int(eventId),
                            "outbreakStart": outbreakStart,
                            "outbreakEnd": outbreakEnd,
                            "description": desc,
                            "duration": duration,
                        }

                        logger.info(f"MERGE event ID: ({eventId})")
                        SESSION.run(event_query, event_params)

                        # Start getting taxon data
                        # Connect to NCBI using serotype if it's available
                        # Otherwise use pathogen name
                        subtype = event["subType"]
                        serotype = "NA"
                        pathogen_ncbi = None
                        if subtype and "disease" in subtype:
                            sero = subtype["disease"]
                            if sero and "name" in sero:
                                serotype = sero["name"]
                                pathogen_ncbi = wahis.search_and_merge(
                                    serotype, SESSION
                                )

                        pathogen_mapping = {
                            "Equine infectious anaemia virus  ": "Equine infectious anemia virus",
                            "Eastern equine encephalomyelitis and Western equine encephalomyelitis viruses ": "Alphavirus",
                            "Paramyxovirus type 1 (PMV-1)": "Pigeon paramyxovirus 1",
                            "H5N8": "H5N8 subtype",
                        }

                        if not pathogen_ncbi:
                            path = event["causalAgent"]
                            if path and "name" in path:
                                pathogen = path["name"]
                                if pathogen in pathogen_mapping:
                                    fixed_path = pathogen_mapping[pathogen]
                                    pathogen_ncbi = wahis.search_and_merge(
                                        fixed_path, SESSION
                                    )
                                else:
                                    pathogen_ncbi = wahis.search_and_merge(
                                        pathogen, SESSION
                                    )

                        if pathogen_ncbi:
                            pathogen_ncbi_id = int(pathogen_ncbi)

                        # Create the event/host Taxon relationship
                        caseCount = "NA"
                        deathCount = "NA"
                        speciesName = None
                        speciesWild = "NA"

                        species_quantities = outbreak_metadata["speciesQuantities"]
                        if species_quantities:
                            # iterate over each host species type and retrieve metadata (name, cases, etc.)
                            for item in species_quantities:
                                newQuantities = item["newQuantities"]
                                if newQuantities:
                                    speciesName = newQuantities["speciesName"]
                                    speciesWild = newQuantities["isWild"]
                                    caseCount = newQuantities["cases"]
                                    deathCount = newQuantities["deaths"]
                        else:
                            quantData = metadata["quantitativeData"]
                            if quantData:
                                newQuantData = quantData["news"]
                                if newQuantData:
                                    for key in newQuantData:
                                        speciesName = key["speciesName"]
                                        speciesWild = key["isWild"]
                                        caseCount = key["cases"]
                                        deathCount = key["deaths"]
                            else:
                                quantData = None
                                logger.info(
                                    f"No quant data for outbreak {eventId} or report {reportId}"
                                )

                        # Check if caseCount and deathCount are available
                        if caseCount is not None and caseCount != "NA":
                            caseCount = int(caseCount)
                        else:
                            caseCount = "NA"

                        if deathCount is not None and deathCount != "NA":
                            deathCount = int(deathCount)
                        else:
                            deathCount = "NA"

                        # If there's a speciesName for the host, continue with NCBI search
                        if speciesName:
                            host_ncbi = wahis.search_and_merge(speciesName, SESSION)
                            if host_ncbi is not None:
                                host_ncbi_id = int(host_ncbi)

                                host_query = """
                                    MATCH (th:Taxon {taxId: $host_ncbi_id})
                                    MATCH (e:Event:Outbreak {eventId: $eventId})
                                    MERGE (e)-[:INVOLVES {
                                        caseCount: $caseCount,
                                        deathCount: $deathCount,
                                        role: 'host',
                                        speciesWild: $speciesWild,
                                        detectionDate: $detectionDate
                                    }]->(th)
                                    WITH e
                                    MATCH (tp:Taxon {taxId: $pathogen_ncbi_id})
                                    MERGE (e)-[:INVOLVES {
                                        role: "pathogen",
                                        detectionDate: $detectionDate,
                                        subtype: $subtype
                                    }]->(tp)
                                    WITH e
                                    MATCH (g:Geography {geonameId: $geonameId})
                                    MERGE (e)-[:OCCURS_IN]->(g)
                                """

                                host_params = {
                                    "uqReportId": uqReportId,
                                    "eventId": int(eventId),
                                    "host_ncbi_id": host_ncbi_id,
                                    "caseCount": caseCount,
                                    "deathCount": deathCount,
                                    "speciesWild": speciesWild,
                                    "geonameId": geonameId,
                                    "pathogen_ncbi_id": pathogen_ncbi_id,
                                    "detectionDate": outbreakStart,
                                    "detectionDateDetails": "Outbreak start date",
                                    "subtype": serotype,
                                }

                                logger.info(f"MERGE host: {speciesName}")
                                SESSION.run(host_query, host_params)

                        # Otherwise, do the pathogen only
                        else:
                            path_query = """
                                    MATCH (e:Event:Outbreak {eventId: $eventId})
                                    MATCH (tp:Taxon {taxId: $pathogen_ncbi_id})
                                    MERGE (e)-[:INVOLVES {
                                        role: "pathogen",
                                        detectionDate: $detectionDate,
                                        subtype: $subtype
                                    }]->(tp)
                                    WITH e
                                    MATCH (g:Geography {geonameId: $geonameId})
                                    MERGE (e)-[:OCCURS_IN]->(g)
                                """

                            path_params = {
                                "uqReportId": uqReportId,
                                "eventId": int(eventId),
                                "geonameId": geonameId,
                                "pathogen_ncbi_id": pathogen_ncbi_id,
                                "detectionDate": outbreakStart,
                                "detectionDateDetails": "Outbreak start date",
                                "subtype": serotype,
                            }

                            logger.info(
                                f"MERGE pathogen only, taxId: {pathogen_ncbi_id}"
                            )
                            SESSION.run(path_query, path_params)

                    except KeyError as e:
                        logger.error(
                            f"KeyError occurred: {e}. Skipping to next outbreak event."
                        )
                        continue

        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"Error decoding JSON response: {e}")
            continue

        except Exception as e:
            if eventId is not None:
                if reportId is not None:
                    logger.info(f"Stopped at eventId {eventId} and reportId {reportId}")

            logger.error(f"An exception occurred: {e}")
            raise
