import pytest
from network.geo_api import GeonamesApi


@pytest.fixture(scope="module")
def set_up_geonames_api():
    geonames_api = GeonamesApi()
    return geonames_api


@pytest.mark.geonames
def test_search_id(geonames_api):
    pass
