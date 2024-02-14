<h1 align="center">
  PIPP Knowledge Graph
</h1>

This project contains source code for the ETL supporting the ingestion of data into Neo4j. Developed on Python 3.9 and Neo4j 5.10.

## Deployment

1. Create a virtual environment

```
python3 -m venv env
```

2. Install required python modules

```
source env/bin/activate
pip3 install -r requirements.txt
```

3. Set environmental variables in file .env. Some folders may need to be unzipped, and you may need to install and set up a Neo4j database (https://neo4j.com/docs/operations-manual/current/installation/) and GeoNames account (http://www.geonames.org/).

```
NEO4J_URI=<neo4j_uri>
NEO4J_USER=<neo4j_user>
NEO4J_PASSWORD=<neo4j_password>
NEO4J_DATABASE=<neo4j_database>
GEO_USER=<geonames_user>
NCBI_API_KEY=<api_key>
```

4. Create taxa and geographical constraints

```
CREATE CONSTRAINT taxId_UQ FOR (taxon:Taxon) REQUIRE taxon.taxId IS UNIQUE
CREATE CONSTRAINT geonameId_UQ FOR (geography:Geography) REQUIRE geography.geonameId IS UNIQUE
```

5. Create knowledge graph locally

```
python main.py
```

## Development and testing

All source code is designed to stop after hitting an error. The most common are API related, usually triggered by reaching credit limits, malformatted API responses, or excess throttling. Batch sizes can trigger errors in Neo4j in rare occasions, change the batch size according to your hardware.

### Unit tests coverage

```
coverage run pytest -vm unit
```

### Integrity tests coverage

```
coverage run pytest -vm integrity
```

### Timing execution

Set the level to DEBUG in main.py

```
logger.add(sys.stderr, level="DEBUG")
```
