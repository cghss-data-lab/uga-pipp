import pytest
from network.ncbi_api import NCBIApi


@pytest.fixture(scope="module")
def set_up_ncbi_api():
    ncbi_api = NCBIApi()
    return ncbi_api


@pytest.mark.ncbi
def test_search(ncbi_api):
    pass
