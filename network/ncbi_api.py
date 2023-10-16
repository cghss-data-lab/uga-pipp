import aiohttp
from bs4 import BeautifulSoup, Tag
from loguru import logger
from cache.cache import cache

ID_SEARCH_CACHE_FILE = "ncbi/cache/id_search.pickle"
METADATA_CACHE_FILE = "ncbi/cache/metadata.pickle"
SUBTREE_CACHE_FILE = "ncbi/cache/subtree.pickle"


class NCBIApi:
    @cache(ID_SEARCH_CACHE_FILE, is_class=True)
    async def search_id(self, name):
        """Get ID from text search, using NCBI esearch eutil"""
        logger.info(f"Searching NCBI for term {name}")

        params = {"db": "Taxonomy", "term": name}
        soup = await self._api_soup("esearch", params)

        try:
            ncbi_id = soup.find("Id")
            if ncbi_id is not None:
                ncbi_id = ncbi_id.getText()
            else:
                logger.warning("NCBI ID not found for the given term.")
                return None

        except AttributeError:
            errors = soup.find("ErrorList")
            warnings = soup.find("WarningList")

            for error in errors.children:
                logger.error(f"{error.name}: {error.getText()}")

            for warning in warnings.children:
                logger.warning(f"{warning.name}: {warning.getText()}")

            return None

        return ncbi_id

    @cache(METADATA_CACHE_FILE, is_class=True)
    async def search_hierarchy(self, ncbi_id, source: str = "NCBI Taxonomy"):
        """Request metadata by NCBI taxonomy ID, and return cleaned object"""

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

        taxon = extract_metadata(soup.TaxaSet.Taxon)

        taxon_set = soup.TaxaSet.Taxon.LineageEx.find_all("Taxon")
        taxon_set = [extract_metadata(taxon) for taxon in taxon_set]

        return taxon_set.append(taxon)

    async def _api_soup(self, eutil, parameters):
        """
        Retrieve NCBI Eutils response XML, and
        parse it into a beautifulsoup object
        """
        base_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/{eutil}.fcgi"

        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(base_url, params=parameters) as response:
                return BeautifulSoup(await response.text(), features="xml")
