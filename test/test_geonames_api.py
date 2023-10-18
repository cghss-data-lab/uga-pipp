import pytest
from network.geo_api import GeonamesApi


@pytest.fixture(scope="module")
def set_up_geonames_api():
    geonames_api = GeonamesApi()
    return geonames_api


@pytest.mark.geonames("name, final", ("France", "seglns"), (3017382, None))
async def test_search_geoname_id(geo_api, name, final):
    results = await geo_api.search_geonames_id(name)
    assert results.get("geonameId") == final


@pytest.mark.geonames("id, final")
async def test_search_hierarchy(geo_api, geoid, final):
    results = await geo_api.search_hierarchy(geoid)
    assert results.get("geonameId") == final


@pytest.mark.geonames("location, final")
async def test_search_lat_long(geo_api, location, final):
    results = await geo_api.search_lat_long(location)
    assert results.get("geonameId") == final
