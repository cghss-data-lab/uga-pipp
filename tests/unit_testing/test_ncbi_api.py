import pytest
from network.ncbi_api import NCBIApi, NCBIApiError


@pytest.fixture(scope="module")
def ncbiapi():
    ncbi_api = NCBIApi()
    return ncbi_api


@pytest.mark.asyncio
@pytest.mark.network
@pytest.mark.ncbi
@pytest.mark.parametrize("name, final", [("Blastocerus dichotomus", 248133)])
async def test_success_search_id(ncbiapi, name, final):
    result = await ncbiapi.search_id(name)
    assert result == final


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.ncbi
@pytest.mark.parametrize("name, final", [("fklgsd", None)])
async def test_fail_search_id(ncbiapi, name, final):
    result = await ncbiapi.search_id(name)
    assert result == final


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.ncbi
@pytest.mark.parametrize("taxid, final", [(9606, 31)])
async def test_success_search_hierarchy(ncbiapi, taxid, final):
    result = await ncbiapi.search_id(taxid)
    assert isinstance(result, list)
    assert len(result) == final


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.ncbi
@pytest.mark.parametrize("taxid, final", [(96394106, None)])
async def test_fail_search_hierarchy(ncbiapi, taxid, final):
    result = await ncbiapi.search_id(taxid)
    assert result == final


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.ncbi
@pytest.mark.parametrize("taxid", [("s;dflmwg",)])
async def test_ncbi_error(ncbiapi, taxid):
    with pytest.raises(NCBIApiError):
        await ncbiapi.search_id(taxid)
