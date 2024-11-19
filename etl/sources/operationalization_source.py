from etl.core.source import Source
from etl.utils.models import Neo4JConfig
from neo4j import GraphDatabase
import pandas as pd
from dataclasses import dataclass


@dataclass
class OperationalizationData:
    df_encuestas: pd.DataFrame
    bibliotecas_id: list
    driver: GraphDatabase.driver


class OperationalizationSource(Source):
    def __init__(self, neo4j_config: Neo4JConfig, survey_path: str):
        self.driver = GraphDatabase.driver(
            neo4j_config.uri,
            auth=(neo4j_config.user, neo4j_config.password)
        )
        self.survey_path = survey_path

    def extract(self) -> OperationalizationData:
        df_encuestas = pd.read_csv(self.survey_path)
        bibliotecas_id = df_encuestas["BibliotecaID"].unique()

        return OperationalizationData(
            df_encuestas=df_encuestas,
            bibliotecas_id=bibliotecas_id,
            driver=self.driver
        )
