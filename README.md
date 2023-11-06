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

3. Create knowledge graph locally

```
python main.py
```

## Development and testing

All source code is designed to stop after hitting an error. The most common are API related, usually triggered by reaching credit limits, malformatted API responses, or excess throttling.

### Network coverage

```
coverage run pytest -vm network
```

### Cache coverage

```
coverage run pytest -vm cache
```

### Timing execution

Set in main.py

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
