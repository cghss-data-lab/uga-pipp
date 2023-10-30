<h1 align="center">
  PIPP Knowledge Graph
</h1>

This project contains source code for the ETL supporting the ingestion of data into Neo4j. Current stage: Only supports local builds

## 👩‍💻 Deployment Quick start

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

### 1. Test coverage after changes to network code

```
coverage run pytest -vm network
```

### 1. Test coverage after changes to cache code

```
coverage run pytest -vm cache
```
