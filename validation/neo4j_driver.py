from neo4j import GraphDatabase


# Manage connection to database through a single object.
class Neo4jDatabase:
    def __init__(self, uri: str, database: str, user: str, password: str) -> None:
        self.db = database
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        if self.driver is not None:
            self.driver.close()

    def run_query(self, query: str):
        with self.driver.session() as session:
            result = session.run(query)
            return list(result)
