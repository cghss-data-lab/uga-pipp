import re

LABEL = "Geography"
FCODE_TO_LABEL = {
    "CONT": "Continent",
    "PCLI": "Country",
    "ADM1": "ADM1",
    "ADM2": "ADM2",
    "ADM3": "ADM3",
    "ADM4": "ADM4",
    "ADM5": "ADM5",
}


def process_parameters(geoname_id, metadata, geonames_api) -> dict:
    lat = metadata.get("lat")
    long = metadata.get("lng")
    iso2 = metadata.get("countryCode", None)

    parameters = {
        "dataSource": "GeoNames",
        "geonameId": int(geoname_id),
        "name": metadata.get("name"),
        "adminCode1": metadata.get("adminCodes1", {}).get("ISO3166_2", "NA"),
        "adminType": metadata.get("adminTypeName", "NA"),
        "iso2": metadata.get("countryCode", "NA"),
        "fclName": metadata.get("fclName", "NA"),
        "fcodeName": metadata.get("fcodeName", "NA"),
        "lat": float(lat) if lat is not None else "NA",
        "long": float(long) if long is not None else "NA",
        "elevation": metadata.get("elevation", "NA"),
        # "polygon": search_for_polygon(geonameId),
    }
    if metadata.get("fcode") == "PCLI":
        parameters["iso3"] = geonames_api.get_iso(iso2)
    if metadata.get("fcode") not in FCODE_TO_LABEL:
        parameters["fcode"] = metadata.get("fcode")
    return parameters


def format_value(value):
    if isinstance(value, str):
        return f"'{value}'"
    if value is None:
        return "NULL"
    return value


def create_properties(parameters: dict) -> str:
    properties = ", ".join(f"{k}: {format_value(v)}" for k, v in parameters.items())
    return properties


def process_node(geoname_id, geonames_api, label=LABEL):
    metadata = geonames_api.get_geo_data(geoname_id)
    parameters = process_parameters(geoname_id, metadata, geoname_id)
    parameters = create_properties(parameters)

    fcode = metadata.get("fcode")
    if fcode in FCODE_TO_LABEL:
        fcode = FCODE_TO_LABEL[fcode]
        label += f":{fcode}"

    return parameters, label
