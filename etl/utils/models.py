import os
from dataclasses import dataclass


@dataclass
class Neo4JConfig:
    """
    Configuration class for Neo4J database connection.
    """
    uri: str = os.getenv("NEO4J_URI")
    user: str = os.getenv("NEO4J_USER")
    password: str = os.getenv("NEO4J_PASSWORD")
