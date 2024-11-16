import os
from dataclasses import dataclass
from typing import List, Dict
import pandas as pd
from neo4j import GraphDatabase
from enum import Enum
from abc import ABC, abstractmethod


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


class AnalysisCategory(Enum):
    CATALOGO_DIGITALIZACION = "Nivel de avance en la digitalización del catálogo"
    SISTEMATIZACION_SERVICIOS = "Nivel de avance en la sistematización de servicios bibliotecarios"
    COLECCION_CARACTERIZACION = "Caracterización de la colección"


class AnalysisCoordinate(ABC):
    def __init__(self, driver, name: str, description: str):
        self.driver = driver
        self.category = None
        self.name = name
        self.description = description
        self.score = None

    @abstractmethod
    def get_data(self) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        raise NotImplementedError


class AnalysisManager:
    def __init__(self, graph_db_config: Neo4JConfig, csv_path: str):
        self.driver = GraphDatabase.driver(
            graph_db_config.uri,
            auth=(graph_db_config.user, graph_db_config.password)
        )
        self.df_encuestas = pd.read_csv(csv_path)
        self.bibliotecas_id = self.df_encuestas["BibliotecaID"].unique()
        self.coordinates: List[AnalysisCoordinate] = []

    def add_category(self, coordinate: AnalysisCoordinate):
        self.coordinates.append(coordinate)

    def run_analysis(self) -> pd.DataFrame:
        results = pd.DataFrame()
        for category in self.coordinates:
            category_results = category.calculate_score(self.bibliotecas_id)
            results = pd.concat([results, category_results], ignore_index=True)
        return results

    @staticmethod
    def save_results(results: pd.DataFrame, output_path: str):
        results.to_csv(output_path, index=False)


class InfraestructuraTecnologicaCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        self.driver = driver
        self.category = AnalysisCategory.CATALOGO_DIGITALIZACION.value
        self.name = "Infraestructura tecnológica"
        self.description = "Disponibilidad de internet y computador en la biblioteca"

    def get_data(self) -> pd.DataFrame:
        with self.driver.session() as session:
            result = session.run(Neo4JQueryManager.infraestructura_tecnologica())
            return pd.DataFrame(result.data())

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data["infraestructura_tecnologica"] = data.apply(
            lambda row: 2 if row["tieneComputador"] and row["tieneConectividad"] else
            (1 if row["tieneComputador"] or row["tieneConectividad"] else 0), axis=1)
        return data


class EstadoDigitalizacionCatalogoCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        self.driver = driver
        self.category = AnalysisCategory.CATALOGO_DIGITALIZACION.value
        self.name = "Estado de la digitalización del catálogo"
        self.description = "Nivel de desarrollo en la digitalización del catálogo bibliográfico"

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'tipo_catalogo']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]

        # Mapping for catalog types to scores
        catalog_scores = {
            'No tiene catálogo': 0,
            'Catálogo analógico': 1,
            'Catálogo en hoja de cálculo': 2,
            'Software Bibliográfico': 3
        }

        data['Puntaje'] = data['tipo_catalogo'].map(catalog_scores)
        return data


class PorcentajeColeccionCatalogoCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        self.driver = driver
        self.category = AnalysisCategory.CATALOGO_DIGITALIZACION.value
        self.name = "% Colección ingresada al catalogo"
        self.description = "Porcentaje de la colección bibliográfica que ha sido catalogada aproximadamente"

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'porcentaje_coleccion_catalogada']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]

        # Score is directly the percentage (0-100)
        data['Puntaje'] = data['porcentaje_coleccion_catalogada']

        return data


class NivelInformacionCatalogoCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        self.driver = driver
        self.category = AnalysisCategory.CATALOGO_DIGITALIZACION.value
        self.name = "Nivel de información capturada en el catálogo"
        self.description = "Detalle de la información capturada en el catálogo"

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'nivel_detalle_catalogo']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]

        # Mapping for information detail levels to scores
        detail_scores = {
            'Sin información': 0,
            'Descripción del material': 1,
            'Sistemas de Clasificación': 2,
            'Estado de disponibilidad de los materiales': 3
        }

        data['Puntaje'] = data['nivel_detalle_catalogo'].map(detail_scores)
        return data


def main():
    db_config = Neo4JConfig()
    analyzer = AnalysisManager(db_config, "data/Contacto Bibliotecas - Formulario Coordenadas.csv")
    catalogo_digitalizacion = AnalysisCoordinate(analyzer.driver, "Nivel de avance en la digitalización del catálogo.")
    catalogo_digitalizacion.add_coordinate(InfraestructuraTecnologicaCoordinate(analyzer.driver))

    pass


if __name__ == '__main__':
    main()
