import pytest
from src.wahis.wahis_api import WAHISApi


@pytest.fixture(scope="module")
def wahisapi():
    wahis_api = WAHISApi()
    return wahis_api


@pytest.mark.asyncio
@pytest.mark.wahis
@pytest.mark.parametrize("event_id, final", [()])
def test_success_search_evolution(wahisapi):
    pass


@pytest.mark.asyncio
@pytest.mark.wahis
@pytest.mark.parametrize("event_id, final", [()])
def test_fail_search_evolution(wahisapi):
    pass


@pytest.mark.asyncio
@pytest.mark.wahis
@pytest.mark.parametrize("report_id, final", [()])
def test_success_search_report(wahisapi):
    pass


@pytest.mark.asyncio
@pytest.mark.wahis
@pytest.mark.parametrize("report_id, final", [()])
def test_fail_search_report(wahisapi):
    pass
