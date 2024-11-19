from typing import List, Dict
from etl.core.base import DataTransformer
from etl.neo4j_import_db_SiBiBo import crear_objetos_neo4j


class BibliotecasTransformer(DataTransformer):
    """
    Transformer for Bibliotecas Comunitarias data.
    Handles the specific transformation logic for library data before loading into Neo4j.
    """

    def transform(self, data: List[Dict]) -> List[Dict]:
        """
        Transforms raw library data into Neo4j compatible format.

        Args:
            data (List[Dict]): Raw data from the source

        Returns:
            List[Dict]: Transformed data ready for Neo4j import
        """
        transformed_data = []

        for row in data:
            transformed_row = crear_objetos_neo4j(row)
            transformed_data.append(transformed_row)

        return transformed_data

    @staticmethod
    def validate_data(row: Dict) -> bool:
        """
        Validates a single data row before transformation.

        Args:
            row (Dict): Single row of data to validate

        Returns:
            bool: True if data is valid, False otherwise
        """
        required_fields = ["nombre_biblioteca", "localidad", "barrio"]
        return all(field in row and row[field] for field in required_fields)
