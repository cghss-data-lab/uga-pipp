
<h1 align="center">
  PIPP Knowledge Graph
</h1>

This project contains source code for the ETL supporting the ingestion of data into Neo4j, and all data and analysis for the paper below. Developed on Python 3.9 and Neo4j 5.10. Please note that this repository makes use of data from the [WAHIS platform](https://wahis.woah.org/#/home) which requires the following statement: “WOAH bears no responsibility for the integrity or accuracy of the data contained herein, in particular due, but not limited to, any deletion, manipulation, or reformatting of data that may have occurred beyond its control.”

## Getting started
This repository is organized into several folders: [`archive`](./archive/), [`cache`](./cache/), [`data`](./data/), [`database_dump`](./database_dump/), [`manuscript_figures`](./manuscript_figures/), [`network`](./network/),[`src`](./src/), and [`tests`](./tests/). Some of these folders contain subfolders and zipped files. All zipped files will need to be unzipped before proceedings. 

Building the knowledge graph and running the queries is not necessary to reproduce visualized results, as the results from the queries (and the full knowledge graph) are provided as .csv files. All data and figures are provided in the [`manuscript_figures`](./manuscript_figures/) directory. A separate README is available in that directory.

If you wish to build the graph and run the queries, you may proceed one of two ways: restore from a database dump or build the knowledge graph locally. For either method, you will need to download and install Neo4j [(here)](https://neo4j.com/download/). 

## Restore from dump
1. Download the [`hpai_kg_backup.dump`](./database_dump/hpai_kg_backup.dump) file from the [`database_dump`](./database_dump/) directory.
2. Open Neo4j, creating a new project if needed.
3. Click into the project and then click "Reveal files" (in your directory or Finder).
4. Move the downloaded database dump into the project folder you just opened.
5. Navigate back to Neo4j, where the `.dump` file should now appear under files.
6. Hover over the file, click the three dots to the right, and select `"Create new DBMS from dump."`
7. Name and set a password for your new database
8. The database should load. You will be able to start the DBMS with the blue `"Open"` button to open the Neo4j browser, viewing the graph as it is structured by clicking the node/relationship labels on the left pane or by using CYPHER to write queries.

## Building the knowledge graph locally
### Deployment

1. Create a virtual environment

```
python3 -m venv env
```

2. Install required python modules

```
source env/bin/activate
pip3 install -r requirements.txt
```

3. Set environmental variables in file .env. Some folders may need to be unzipped, and you may need to install and set up a Neo4j database (https://neo4j.com/docs/operations-manual/current/installation/). You will also need to create an NCBI (https://account.ncbi.nlm.nih.gov/) account with an API key and a GeoNames account (http://www.geonames.org/).

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

### Development and testing

All source code is designed to stop after hitting an error. The most common are API related, usually triggered by reaching credit limits, malformatted API responses, or excess throttling. Batch sizes can trigger errors in Neo4j in rare occasions, change the batch size according to your hardware.

#### Unit tests coverage

```
coverage run pytest -vm unit
```

#### Integrity tests coverage

```
coverage run pytest -vm integrity
```

#### Timing execution

Set the level to DEBUG in main.py

```
logger.add(sys.stderr, level="DEBUG")
```
