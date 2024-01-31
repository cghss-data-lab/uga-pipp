from loguru import logger
from tests.timer import timer
from src.wahis.valid_wahis import valid_wahis
from network.handle_concurrency import handle_concurrency
import json
from devtools import debug


@timer
async def ingest_wahis(
    database_handler,
    geoapi,
    ncbiapi,
    batch_size=1000,
    query_path="src/wahis/wahis.cypher",
) -> None:
    logger.info("Ingesting WAHIS")
    wahis, geonames, tax_names, tax_ids = await valid_wahis(geoapi, ncbiapi)

    tax_hierarchies = await handle_concurrency(
        *[ncbiapi.search_hierarchy(taxon) for taxon in tax_ids]
    )

    taxons = dict(zip(tax_names, tax_hierarchies))

    for row in wahis:
        # Store processed host dictionaries in list
        processed_hosts = []

        # Determine the host name
        species_quantities = row['quantitativeData']['news']
        species_totals = row['quantitativeData']['totals']
        if species_quantities:
            for species in species_quantities:
                host_search_name = species['speciesName']
                # Process each host using ncbiapi.process_taxon and append to processed_hosts
                processed_host = ncbiapi.process_taxon(host_search_name, taxons)
                # Add additional properties to the host dictionary
                processed_host['processed'] = "NA"
                processed_host['positive'] = species['cases']
                processed_host['deaths'] = species['deaths']
                processed_host['observation_type'] = "Case report"
                processed_host['observation_date'] = row['event']['confirmOn']
                processed_host['species_wild'] = species['isWild']
                processed_hosts.append(processed_host)
        elif species_totals:
            for species in species_totals:
                host_search_name = species['speciesName']
                processed_host = ncbiapi.process_taxon(host_search_name, taxons)
                processed_host['processed'] = "NA"
                processed_host['positive'] = species['cases']
                processed_host['deaths'] = species['deaths']
                processed_host['observation_type'] = "Case report"
                processed_host['observation_date'] = row['event']['confirmOn']
                processed_host['species_wild'] = species['isWild']
                processed_hosts.append(processed_host)
        else:
            host_search_name = row['event']['disease']['group']
            processed_host = ncbiapi.process_taxon(host_search_name, taxons)
            processed_host['processed'] = "NA"
            processed_host['positive'] = "NA"
            processed_host['deaths'] = "NA"
            processed_host['observation_type'] = "Case report"
            processed_host['observation_date'] = row['event']['confirmOn']
            processed_host['species_wild'] = "NA"
            processed_hosts.append(processed_host)

        # Assign the processed_hosts list to the "hosts" key in the row
        row["hosts"] = processed_hosts
    
        # Determine the pathogen name
        subtype = row['event']['subType']
        path_search_name = None
        if subtype and 'disease' in subtype:
            sero = subtype['disease']
            if sero and 'name' in sero:
                path_search_name = sero['name']
        
        if not path_search_name:
            path = row['causalAgent']
            if path and 'name' in path:
                path_search_name = path['name']

        row["pathogen"] = ncbiapi.process_taxon(
            path_search_name, taxons
        )
        row["outbreak"]["geonames"] = geonames[row["outbreak"]["geonames"]]

    geo_hierarchies = await handle_concurrency(
        *[geoapi.search_hierarchy(geoid["geonameId"]) for geoid in geonames.values()]
    )

    batches = (len(wahis) - 1) // batch_size + 1
    for i in range(batches):
        batch = wahis[i * batch_size : (i + 1) * batch_size]
        debug(batch[0])
        await database_handler.execute_query(query_path, properties=batch)

    await handle_concurrency(
        *[
            database_handler.build_geohierarchy(hierarchy)
            for hierarchy in geo_hierarchies
        ]
    )

    await handle_concurrency(
        *[
            database_handler.build_ncbi_hierarchy(hierarchy)
            for hierarchy in tax_hierarchies
        ]
    )
