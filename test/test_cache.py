import pickle
import pytest
from cache.cache import cache


@pytest.fixture(scope="module")
@cache("test/test.pickle")
def mock_function(n):
    return n + 1


@pytest.mark.asyncio
@pytest.mark.cache
@pytest.mark.parametrize("", [])
async def test_cache(mock_function):
    pass
