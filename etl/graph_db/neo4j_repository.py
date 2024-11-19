from neo4j import GraphDatabase, exceptions

from etl.utils.utils import setup_logger

logger = setup_logger("neo4j_repository.log")


class Neo4JRepository:
    """
    Repository class to encapsulate Neo4j database interactions.
    """

    def __init__(self, uri: str, user: str, password: str):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        logger.info("Neo4jRepository initialized with URI: %s", uri)

    def close(self):
        """
        Close the database driver connection.
        """
        self._driver.close()
        logger.info("Neo4jRepository connection closed")

    def execute_write(self, transaction_function, *args, **kwargs):
        """
        Execute a write query within a transaction.
        """
        with self._driver.session() as session:
            try:
                result = session.execute_write(transaction_function, *args, **kwargs)
                return result
            except exceptions.Neo4jError as e:
                logger.error("Neo4j write transaction failed: %s", e)
                raise

    def execute_read(self, transaction_function, *args, **kwargs):
        """
        Execute a read query within a transaction.
        """
        with self._driver.session() as session:
            try:
                result = session.execute_read(transaction_function, *args, **kwargs)
                return result
            except exceptions.Neo4jError as e:
                logger.error("Neo4j read transaction failed: %s", e)
                raise
