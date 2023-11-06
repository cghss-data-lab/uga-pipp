import os
import json
import aiohttp
from bs4 import BeautifulSoup, Tag
from loguru import logger
from cache.cache import cache

NCBI_API_KEY = os.environ["NCBI_API_KEY"]
NCBI_ID_CACHE_FILE = "network/cache/ncbi_id.pickle"
NCBI_HIERARCHY_CACHE_FILE = "network/cache/ncbi_hierarchy.pickle"


class NCBIApiError(Exception):
    def __init__(self, value, message):
        self.value = value
        self.message = message
        super().__init__(message)


class NCBIApi:
    @cache(NCBI_ID_CACHE_FILE, is_class=True)
    async def search_id(self, name: str) -> int:
        """Get ID from text search, using NCBI esearch eutil"""
        logger.debug(f"Searching NCBI for term {name}")

        if not name:
            return

        params = {"db": "Taxonomy", "term": name}
        soup = await self._api_soup("esearch", params)

        if not soup:
            return

        ncbi_id = soup.Id
        if not ncbi_id:
            logger.warning(f"NCBI ID not found for the given {name}.")
            return ncbi_id

        return ncbi_id.getText()

    @cache(NCBI_HIERARCHY_CACHE_FILE, is_class=True)
    async def search_hierarchy(
        self, ncbi_id: int, source: str = "NCBI Taxonomy"
    ) -> list:
        """Request metadata by NCBI taxonomy ID, and return cleaned object"""
        logger.debug(f"Searching hierarchy for NCBI ID {ncbi_id}")

        if not ncbi_id:
            return

        def extract_metadata(taxon: Tag) -> dict:
            taxon_metadata = {
                "taxId": taxon.TaxId.getText(),
                "name": taxon.ScientificName.getText(),
                "rank": taxon.Rank.getText(),
                "dataSource": source,
            }
            return taxon_metadata

        params = {"db": "Taxonomy", "id": ncbi_id}
        soup = await self._api_soup("efetch", params)

        if not soup or not soup.TaxaSet or not soup.TaxaSet.Taxon:
            return

        taxon = extract_metadata(soup.TaxaSet.Taxon)

        taxon_set = soup.TaxaSet.Taxon.LineageEx.find_all("Taxon")
        taxon_set = [extract_metadata(taxon) for taxon in taxon_set]
        taxon_set.append(taxon)
        return taxon_set

    async def _api_soup(self, eutil: str, parameters: dict) -> BeautifulSoup:
        """
        Retrieve NCBI Eutils response XML, and
        parse it into a beautifulsoup object
        """
        base_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/{eutil}.fcgi"

        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(
                base_url, params={"api_key": NCBI_API_KEY, **parameters}
            ) as response:
                result = await response.text()
                result.replace("\n", "")

                if "error" in result:
                    try:
                        error = json.loads(result)
                        raise NCBIApiError(value=parameters, message=error["error"])
                    except json.JSONDecodeError:
                        logger.warning(result)
                        return

                return BeautifulSoup(result, features="xml")

    @staticmethod
    def process_taxon(name: str, mapping: dict):
        if not mapping[name]:
            return None
        return mapping[name][-1]
