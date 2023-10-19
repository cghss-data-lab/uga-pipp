import pytest
from network.geo_api import GeonamesApi, GeonamesApiError


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
async def test_fails_search_geoname_id(geoapi, name, final):
    results = await geoapi.search_geoname_id(name)
    assert results == final


@pytest.mark.asyncio
@pytest.mark.geonames
@pytest.mark.parametrize("geoid, final", [(3017382, 3), (298795, 3), (7733353, 5)])
async def test_success_search_hierarchy(geoapi, geoid, final):
    results = await geoapi.search_hierarchy(geoid)
    assert len(results) == final


@pytest.mark.asyncio
@pytest.mark.geonames
@pytest.mark.parametrize("geoid", [("3248fn",)])
async def test_fail_search_hierarchy(geoapi, geoid):
    with pytest.raises(GeonamesApiError):
        await geoapi.search_hierarchy(geoid)


@pytest.mark.asyncio
@pytest.mark.geonames
@pytest.mark.parametrize(
    "location, final", [((1.32, -65.21), 8458367), ((1.32, -65.21), 8458367)]
)
async def test_success_search_lat_long(geoapi, location, final):
    results = await geoapi.search_lat_long(location)
    assert results.get("geonameId") == final


@pytest.mark.asyncio
@pytest.mark.geonames
@pytest.mark.parametrize("location, final", [((-74.21, 34.65), None)])
async def test_fail_search_lat_long(geoapi, location, final):
    results = await geoapi.search_lat_long(location)
    assert results == final


@pytest.mark.asyncio
@pytest.mark.geonames
@pytest.mark.parametrize("location", [(-2000.21, 484.65)])
async def test_error_search_lat_long(geoapi, location):
    with pytest.raises(GeonamesApiError):
        await geoapi.search_lat_long(location)
