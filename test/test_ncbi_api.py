import pytest
from network.ncbi_api import NCBIApi


@pytest.fixture(scope="module")
def ncbiapi():
    ncbi_api = NCBIApi()
    return ncbi_api


@pytest.mark.asyncio
@pytest.mark.ncbi
@pytest.mark.parametrize("name, final", [()])
async def test_success_search_id(ncbiapi, name, final):
    result = await ncbiapi.search_id(name)
    assert result == final


@pytest.mark.asyncio
@pytest.mark.ncbi
@pytest.mark.parametrize("name, final", [()])
async def test_fail_search_id(ncbiapi, name, final):
    result = await ncbiapi.search_id(name)
    assert result == final


@pytest.mark.asyncio
@pytest.mark.ncbi
@pytest.mark.parametrize("taxid, final", [()])
async def test_success_search_hierarchy(ncbiapi, taxid, final):
    result = await ncbiapi.search_id(taxid)
    assert result == final


@pytest.mark.asyncio
@pytest.mark.ncbi
@pytest.mark.parametrize("taxid, final", [()])
async def test_fail_search_hierarchy(ncbiapi, taxid, final):
    result = await ncbiapi.search_id(taxid)
    assert result == final
