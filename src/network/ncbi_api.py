import asyncio
import aiohttp
from loguru import logger
from cache.cache import cache

ID_SEARCH_CACHE_FILE = "ncbi/cache/id_search.pickle"
METADATA_CACHE_FILE = "ncbi/cache/metadata.pickle"
SUBTREE_CACHE_FILE = "ncbi/cache/subtree.pickle"


class NCBI:
    @cache(ID_SEARCH_CACHE_FILE, is_class=True)
    async def id_search(self, name):
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
    async def get_metadata(self, ncbi_id, source: str = "NCBI Taxonomy"):
        """Request metadata by NCBI taxonomy ID, and return cleaned object"""

        params = {"db": "Taxonomy", "id": ncbi_id}
        soup = await self._api_soup("efetch", params)

        if not soup.TaxaSet:
            raise ValueError("TaxaSet object not found in the API response.")

        taxon_set = soup.TaxaSet

        if not taxon_set.Taxon:
            raise ValueError("Taxon object not found in the API response.")

        taxon = taxon_set.Taxon

        taxon_metadata = {
            "scientificName": taxon.ScientificName.getText(),
            "parentTaxId": taxon.ParentTaxId.getText(),
            "rank": taxon.Rank.getText(),
            "division": taxon.Division.getText(),
            "geneticCode": {
                "GCId": taxon.GCId.getText(),
                "GCName": taxon.GCName.getText(),
            },
            "mitoGeneticCode": {
                "MGCId": taxon.MGCId.getText(),
                "MGCName": taxon.MGCName.getText(),
            },
            "lineage": taxon.Lineage.getText(),
            "createDate": taxon.CreateDate.getText(),
            "updateDate": taxon.UpdateDate.getText(),
            "pubDate": taxon.PubDate.getText(),
            "dataSource": source,
        }

        if taxon.OtherNames:
            taxon_metadata["otherNames"] = (taxon.OtherNames.getText(),)

        # parse lineage
        lineage_ex = []
        if taxon.LineageEx and taxon.LineageEx.children:
            for taxon_child in taxon.LineageEx.children:
                if isinstance(taxon_child, Tag):
                    lineage_ex.append(
                        {
                            "taxId": taxon_child.TaxId.getText(),
                            "scientificName": taxon_child.ScientificName.getText(),
                            "rank": taxon_child.Rank.getText(),
                            "dataSource": source,
                        }
                    )
        taxon_metadata["lineageEx"] = lineage_ex

        return taxon_metadata

    @cache(SUBTREE_CACHE_FILE, is_class=True)
    async def subtree_search(self, ncbi_id):
        """Get Subtree from text search, using NCBI esearch eutil"""
        logger.info(f"Searching NCBI for ID {ncbi_id}")

        term = f"txid{str(ncbi_id)}[Subtree]"
        params = {"db": "Taxonomy", "term": term}
        soup = await self._api_soup("esearch", params)

        try:
            id_list = []
            id_elements = soup.find_all("Id")
            for id_element in id_elements:
                id_list.append(id_element.getText())

            return id_list

        except AttributeError:
            errors = soup.find("ErrorList")
            warnings = soup.find("WarningList")

            for error in errors.children:
                logger.error(f"{error.name}: {error.getText()}")

            for warning in warnings.children:
                logger.warning(f"{warning.name}: {warning.getText()}")

            return None

    async def _api_soup(self, eutil, parameters):
        """
        Retrieve NCBI Eutils response XML, and
        parse it into a beautifulsoup object
        """
        base_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/{eutil}.fcgi"

        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(base_url, params=parameters) as response:
                return await response.json()
