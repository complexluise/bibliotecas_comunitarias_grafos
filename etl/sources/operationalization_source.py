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
        col_names = [
            "marca_temporal",
            "BibliotecaID",
            "nombre_biblioteca_comunitaria",
            "direccion",
            "barrio",
            "representante",
            "número_contacto",
            "catalogo_digitalización",
            "porcentaje_coleccion_catalogada",
            "nivel_detalle_catalogo",
            "sistemas_clasificacion",
            "nivel_detalle_organizacion_coleccion",
            "tiempo_busqueda_libro",
            "sistema_registro_usuarios",
            "reglamento_servicios",
            "sistematización_prestamo_externo",
            "percepcion_estado_colecciones",
            "enfoques_colecciones",
            "actividades_mediacion",
            "frecuencia_actividades_mediacion",
            "colecciones_especiales",
            "nivel_interes_digitalizacion_koha",
            "nivel_impacto_adoptar_koha",
            "capacidad_tecnica_personal",
            "sobrecarga_admin_catalogo"
        ]
        df_encuestas = pd.read_csv(self.survey_path, header=0, names=col_names)
        bibliotecas_id = df_encuestas["BibliotecaID"].unique()
        return OperationalizationDataSource(
            df_encuestas=df_encuestas, bibliotecas_id=bibliotecas_id, driver=self.driver
        )
