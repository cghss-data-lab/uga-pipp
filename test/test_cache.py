import os
import pytest
from cache.cache import cache, save_cache, load_cache

TEST_CACHE_FILE = "test/test.pickle"


@cache(TEST_CACHE_FILE)
async def mock_function(n):
    return n + 1


@pytest.mark.asyncio
@pytest.mark.cache
async def test_save_cache():
    await save_cache({}, TEST_CACHE_FILE)
    assert os.path.isfile(TEST_CACHE_FILE)


@pytest.mark.asyncio
@pytest.mark.cache
def test_load_cache():
    memoize = load_cache(TEST_CACHE_FILE)
    assert memoize == {}


@pytest.mark.asyncio
@pytest.mark.cache
@pytest.mark.parametrize("n, final", [(1, 2), (90, 91), (10, 11), (4, 5)])
async def test_cache(n, final):
    await mock_function(n)
    assert mock_function.cache[n] == final


# Not a test, but deletes the mock cache file
@pytest.mark.cache
def test_delete():
    os.remove(TEST_CACHE_FILE)
