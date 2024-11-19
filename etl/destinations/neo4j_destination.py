from etl.core.base import DataDestination
from etl.utils.models import Neo4JConfig
from etl.neo4j_import_db_SiBiBo import cargar_datos_en_neo4j


class Neo4jDestination(DataDestination):
    def __init__(self, config: Neo4JConfig):
        self.config = config

    def load(self, data: list[dict]):
        cargar_datos_en_neo4j(
            uri=self.config.uri,
            usuario=self.config.user,
            contraseña=self.config.password,
            datos=data,
        )
