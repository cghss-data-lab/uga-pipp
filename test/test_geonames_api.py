import pytest
from network.geo_api import GeonamesApi


@pytest.fixture(scope="module")
def geoapi():
    geonames_api = GeonamesApi()
    return geonames_api


@pytest.mark.asyncio
@pytest.mark.geonames
@pytest.mark.parametrize("name, final", [("France", 3017382), ("Turkey", 298795)])
async def test_success_search_geoname_id(geoapi, name, final):
    results = await geoapi.search_geoname_id(name)
    assert results.get("geonameId") == final


@pytest.mark.asyncio
@pytest.mark.geonames
@pytest.mark.parametrize("name, final", [("afkbsfkb", None), ("3248fn", None)])
async def test_fail_search_geoname_id(geoapi, name, final):
    results = await geoapi.search_geoname_id(name)
    assert results == final


@pytest.mark.asyncio
@pytest.mark.geonames
@pytest.mark.parametrize("id, final")
async def test_search_hierarchy(geoapi, geoid, final):
    results = await geoapi.search_hierarchy(geoid)
    assert results.get("geonameId") == final


@pytest.mark.asyncio
@pytest.mark.geonames
@pytest.mark.parametrize("location, final")
async def test_search_lat_long(geoapi, location, final):
    results = await geoapi.search_lat_long(location)
    assert results.get("geonameId") == final
