from typing import List, Dict
from neo4j import GraphDatabase
from etl.core.base import DataDestination
from etl.utils.models import Neo4JConfig


class Neo4jDestination(DataDestination):
    def __init__(self, config: Neo4JConfig):
        self.uri = config.uri
        self.username = config.user
        self.password = config.password
        self._driver = None

    @property
    def driver(self):
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                self.uri, auth=(self.username, self.password)
            )
        return self._driver

    def load(self, data: List[Dict]):
        try:
            with self.driver.session() as session:
                for record in data:
                    session.execute_write(self._process_record, record)
        finally:
            self.close()

    def close(self):
        if self._driver:
            self._driver.close()
            self._driver = None

    @staticmethod
    def _process_record(tx, record: Dict):
        for node_type, node_data in record.items():
            if isinstance(node_data, dict):
                Neo4jDestination._create_node(tx, node_type, node_data)
            elif isinstance(node_data, list):
                Neo4jDestination._create_relationships(
                    tx, node_type, node_data, record.get("id")
                )

    @staticmethod
    def _create_node(tx, node_type: str, properties: Dict):
        query = f"""
        MERGE (n:{node_type} {{id: $properties.id}})
        SET n += $properties
        """
        tx.run(query, properties=properties)

    @staticmethod
    def _create_relationships(
        tx, relationship_type: str, related_nodes: List, source_id: str
    ):
        query = f"""
        MATCH (source {{id: $source_id}})
        MERGE (target:{relationship_type} {{name: $target_name}})
        MERGE (source)-[r:{relationship_type}]->(target)
        """
        for node in related_nodes:
            tx.run(query, source_id=source_id, target_name=node.get("name"))
