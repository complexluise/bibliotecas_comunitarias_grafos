from etl.utils.utils import extract_csv, setup_logger
from etl.neo4j_import_db_SiBiBo import crear_objetos_neo4j, cargar_datos_en_neo4j
from etl.utils.models import Neo4JConfig
# Configurations and Constants
NEO4J_CONFIG = {
    "uri": "bolt://localhost:7687",
    "user": "neo4j",
    "password": "password"
}

logger = setup_logger("etl_main.log")


class ETLProcess:
    def __init__(self, neo4j_config: Neo4JConfig):
        self.neo4j_config = neo4j_config
        self.raw_data = {}
        self.transformed_data = {}

    @staticmethod
    def extract(csv_file_path: str) -> list[dict]:
        """Extract data from CSV source"""
        logger.info("Starting data extraction")
        return extract_csv(csv_file_path)

    @staticmethod
    def transform(raw_data: list[dict]) -> list[dict]:
        """Transform raw data into Neo4j compatible format"""
        logger.info("Starting data transformation")
        # TODO filter by field with data.
        return [crear_objetos_neo4j(row) for row in raw_data]

    def load(self, transformed_data: list[dict]):
        """Load data into Neo4j"""
        logger.info("Starting data loading to Neo4j")
        cargar_datos_en_neo4j(
            uri=self.neo4j_config["uri"],
            usuario=self.neo4j_config["user"],
            contraseña=self.neo4j_config["password"],
            datos=transformed_data
        )


def main():
    csv_file_path = '../data/BASE DE DATOS DE BIBLIOTECAS COMUNITARIAS DE BOGOTÁ - SIBIBO 2024 - Base de datos.csv'

    etl = ETLProcess(NEO4J_CONFIG)

    try:
        # Extract
        raw_data = etl.extract(csv_file_path)
        logger.info(f"Extracted {len(raw_data)} records")

        # Transform
        transformed_data = etl.transform(raw_data)
        logger.info(f"Transformed {len(transformed_data)} records")

        # Load
        etl.load(transformed_data)
        logger.info("ETL process completed successfully")

    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
