<h1 align="center">
  PIPP Knowledge Graph
</h1>

This project contains source code for the ETL supporting the ingestion of data into Neo4j. Developed ion Python 3.9 and currently, only for local builds.

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

3. Set environmental variables in file .env

NEO4J_URI=<uri>
NEO4J_USER=<user>
NEO4J_PASSWORD=<pssword>
NEO4J_DATABASE=<database>
GEO_USER=<geonames>
NCBI_API_KEY=<api_key>

4. Create knowledge graph locally

```
python main.py
```

## Development and testing

All source code is designed to stop after hitting an error. The most common are API related, usually triggered by reaching credit limits, malformatted API responses, or excess throttling. Batch sizes can trigger errors in Neo4j in rare occasions, change the batch size according to your hardware.

### Network coverage

```
coverage run pytest -vm network
```

### Cache coverage

```
coverage run pytest -vm cache
```

### Timing execution

Set the level to DEBUG in main.py

```
logger.add(sys.stderr, level="DEBUG")
```

Benchmarks for MacBook Pro 2019 8-Core Intel Core i9

| Dataset  | Time |
| -------- | ---- |
| FluNet   |      |
| GMPD     |      |
| WAHIS    |      |
| Virion   |      |
| Combine  |      |
| WorldPop |      |
| Total    |      |
