import pytest
from src.wahis.wahis_api import WAHISApi, WAHISApiError


@pytest.fixture(scope="module")
def wahisapi():
    wahis_api = WAHISApi()
    return wahis_api


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.wahis
@pytest.mark.parametrize("event_id, final", [(4714, 3), (32, 2)])
async def test_success_search_evolution(wahisapi, event_id, final):
    result = await wahisapi.search_evolution(event_id)
    assert len(result) == final


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.wahis
@pytest.mark.parametrize("event_id, final", [(-23, None), (0, None)])
async def test_fail_search_evolution(wahisapi, event_id, final):
    result = await wahisapi.search_evolution(event_id)
    assert result == final


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.wahis
@pytest.mark.parametrize("event_id", [("soymilk"), ("evolution")])
async def test_error_search_evolution(wahisapi, event_id):
    with pytest.raises(WAHISApiError):
        await wahisapi.search_evolution(event_id)


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.wahis
@pytest.mark.parametrize("report_id, country", [(157894, "Germany"), (1574, "Spain")])
async def test_success_search_report(wahisapi, report_id, country):
    result = await wahisapi.search_report(report_id)
    assert (result["report"]["reportId"], result["event"]["country"]["name"]) == (
        report_id,
        country,
    )


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.wahis
@pytest.mark.parametrize("report_id, final", [(-23, None), (0, None)])
async def test_fail_search_report(wahisapi, report_id, final):
    result = await wahisapi.search_report(report_id)
    assert result == final


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.wahis
@pytest.mark.parametrize("report_id", [("sandwich"), ("13fk94"), ("894$1")])
async def test_error_search_report(wahisapi, report_id):
    with pytest.raises(WAHISApiError):
        await wahisapi.search_report(report_id)
