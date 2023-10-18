import pytest
from network.ncbi_api import NCBIApi


@pytest.fixture(scope="module")
def ncbiapi():
    ncbi_api = NCBIApi()
    return ncbi_api


@pytest.mark.asyncio
@pytest.mark.ncbi
@pytest.mark.parametrize()
def test_search(ncbiapi):
    pass
