from etl.core.base import DataSource
from etl.utils.models import Neo4JConfig
from neo4j import GraphDatabase
import pandas as pd
from dataclasses import dataclass


@dataclass
class OperationalizationDataSource:
    df_encuestas: pd.DataFrame
    bibliotecas_id: list
    driver: GraphDatabase.driver

    def __len__(self) -> int:
        return self.df_encuestas.shape[0]


class OperationalizationSource(DataSource):
    def __init__(self, neo4j_config: Neo4JConfig, survey_path: str):
        self.driver = GraphDatabase.driver(
            neo4j_config.uri, auth=(neo4j_config.user, neo4j_config.password)
        )
        self.survey_path = survey_path

    def extract(self) -> OperationalizationDataSource:
        df_encuestas = pd.read_csv(self.survey_path)
        bibliotecas_id = df_encuestas["BibliotecaID"].unique()
        question_to_nombre_columna = {
            "¿Qué tipo de catálogo se ha desarrollado en la biblioteca comunitaria?": "catalogo_digitalización",
            "¿Qué porcentaje de la colección ha sido ingresada al catalogo aproximadamente?": "porcentaje_coleccion_catalogada",
            "¿Qué nivel de detalle tiene la información registrada en el catálogo?": "nivel_detalle_catalogo",
            "¿Qué sistema de clasificación usa en la colección?": "sistemas_clasificacion",
            "¿Cómo se identifica la ubicación de los libros en la colección?": "nivel_orden_coleccion",
            "¿Cuánto tiempo en promedio tarda en encontrar un libro?": "tiempo_busqueda_libro",
            "¿Cuenta con sistema de registro de usuarios?": "sistema_registro",
            "¿Existe un reglamento de servicios?": "reglamento_servicios",
            "¿Cómo se gestiona los préstamos externos de la colección?": "sistematización_prestamo_externo",
            "¿Cuál es su percepción general del estado de conservación de la colección?": "percepcion_estado_colecciones",
            "¿Las colecciones en su biblioteca tienen un enfoque en particular?": "especificidad_topicos",
            "¿En cuáles actividades de mediación usas la colección?": "actividades_mediacion",
            "¿Con que frecuencia se usa la colección en actividades de mediación?": "frecuencia_mediacion",
            "¿La biblioteca cuenta con colecciones de valor patrimonial?": "colecciones_especiales",
            "Del 1 al 5 ¿Cuál es su nivel de interés en el mapeo y digitalización de la colección en su biblioteca mediante el software Koha?": "nivel_interes_digitalizacion_koha",
            "¿Cuál sería el nivel de impacto al adoptar Koha en la biblioteca?": "nivel_impacto_adoptar_koha",
            "¿Cuál es el nivel de experiencia del personal con sistemas digitales de gestión bibliotecaria?": "capacidad_tecnica_personal",
            "¿Cuántas horas dispondrían a la semana para gestionar el catálogo Koha?": "sobrecarga_admin_catalogo"
        }
        df_encuestas = df_encuestas.rename(columns=question_to_nombre_columna)
        return OperationalizationDataSource(
            df_encuestas=df_encuestas, bibliotecas_id=bibliotecas_id, driver=self.driver
        )
