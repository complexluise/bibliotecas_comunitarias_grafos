import os
from dataclasses import dataclass
from typing import Tuple

import pandas as pd
from neo4j import GraphDatabase
import csv


@dataclass
class Neo4JConfig:
    uri: str = os.getenv("NEO4J_URI")
    user: str = os.getenv("NEO4J_USER")
    password: str = os.getenv("NEO4J_PASSWORD")


class Neo4JQueryManager:
    @staticmethod
    def infraestructura_tecnologica():
        return """MATCH (b:BibliotecaComunitaria)
        OPTIONAL MATCH (b)-[r1:TIENE_TECNOLOGIA]->(t1:TipoTecnologia)
        OPTIONAL MATCH (b)-[r2:USA_TECNOLOGIA]->(t2:Tecnologia) 
        RETURN 
            b.id AS BibliotecaID, 
            b.nombre AS BibliotecaNombre, 
            CASE WHEN t1.nombre = "computadores" THEN true ELSE false END AS tieneComputador,
            t2.conectividad AS tieneConectividad
        """


class AnalysisCategory:
    def __init__(self, driver):
        self.name: str = None
        self.description: str = None
        self.driver = driver

    def get_data(self):
        raise NotImplementedError

    def calculate_score(self, graph_data, csv_data):
        raise NotImplementedError


class TechnologyInfrastructure(AnalysisCategory):
    def __init__(self, driver):
        super().__init__(driver)
        self.name = "Infraestructura tecnológica"
        self.description = "Disponibilidad de internet y computador en la biblioteca"

    def get_data(self) -> pd.DataFrame:
        with self.driver.session() as session:
            result = session.run(Neo4JQueryManager.infraestructura_tecnologica())
            return pd.DataFrame(result.data())

    def calculate_score(self, bibliotecas: list[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data["Puntaje"] = data.apply(
            lambda row: 2 if row["tieneComputador"] and row["tieneConectividad"] else
            (1 if row["tieneComputador"] or row["tieneConectividad"] else 0), axis=1)
        return data


class AnalysisManager:
    def __init__(self, graph_db_config: Neo4JConfig, csv_path):
        self.driver = GraphDatabase.driver(
            graph_db_config.uri,
            auth=(graph_db_config.user, graph_db_config.password),
        )
        self.df_encuestas = pd.read_csv(csv_path)
        self.bibliotecas_id = self.df_encuestas["BibliotecaID"].unique()
        self.categories = []

    def add_category(self, category: AnalysisCategory):
        self.categories.append(category)

    def run_analysis(self) -> pd.DataFrame:
        results = pd.DataFrame()
        for category in self.categories:
            category_results = category.calculate_score(self.bibliotecas_id)
            results = pd.concat([results, category_results], ignore_index=True)

        return results

    def save_results(self, results):
        results.to_csv(self.df_encuestas, index=False)


if __name__ == '__main__':
    db_config = Neo4JConfig()
    analyzer = AnalysisManager(db_config, "data/Contacto Bibliotecas - Formulario Coordenadas.csv")
    analyzer.add_category(TechnologyInfrastructure(analyzer.driver))
    data_categories = analyzer.run_analysis()
    analyzer.save_results(data_categories)


