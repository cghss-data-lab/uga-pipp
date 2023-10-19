import aiohttp


class WAHISApi:
    async def search_evolution(self, eventId):
        evolution_url = f"event/{eventId}/report-evolution?language=en"
        data = await self._wahis_api.get(evolution_url)
        evolution_list = []
        if data:
            for key in data:
                reportId = key["reportId"]
                evolution_list.append(reportId)

            return evolution_list
        else:
            return None

    async def search_outbreak(self, reportId, eventId):
        outbreak_url = f"review/report/{reportId}/outbreak/{eventId}/all-information?language=en"
        return await self._wahis_api.get(outbreak_url)

    async def search_report(self, reportId):
        report_url = f"review/report/{reportId}/all-information?language=en"
        return await self._wahis_api.get(report_url)
        

    async def _wahis_api(self, url) -> dict:
        base_url = f"https://wahis.woah.org/api/v1/pi/{url}
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(base_url) as response:
                return response.json()
