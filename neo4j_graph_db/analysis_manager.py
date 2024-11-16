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

    @staticmethod
    def diversidad_colecciones():
        return """
        MATCH (b:BibliotecaComunitaria)-[:TIENE_COLECCION]->(c:Coleccion)
        RETURN b.id AS BibliotecaID, collect(DISTINCT c.tipo) AS tipos_coleccion
        """

    @staticmethod
    def cantidad_material_bibliografico():
        return """
        MATCH (b:BibliotecaComunitaria)
        RETURN b.id AS BibliotecaID, b.cantidad_inventario AS cantidad_inventario
        """

    @staticmethod
    def diversidad_servicios():
        return """
        MATCH (b:BibliotecaComunitaria)-[:OFRECE_SERVICIO]->(s:Servicio)
        RETURN b.id AS BibliotecaID, collect(DISTINCT s.tipo) AS servicios
        """

    @staticmethod
    def tipos_coleccion():
        return """
        MATCH (b:BibliotecaComunitaria)-[:TIENE_COLECCION]->(c:Coleccion)
        RETURN b.id AS BibliotecaID, collect(DISTINCT c.tipo) AS tipos_coleccion
        """


class AnalysisCategory(Enum):
    CATALOGO_DIGITALIZACION = "Nivel de avance en la digitalización del catálogo"
    SISTEMATIZACION_SERVICIOS = "Nivel de avance en la sistematización de servicios bibliotecarios"
    COLECCION_CARACTERIZACION = "Caracterización de la colección"
    FACILIDAD_ADOPCION_KOHA = "Facilidad de Adopción del Catálogo en Koha"
    IMPACTO_ADOPTAR_KOHA = "Impacto de Adoptar Koha"
    DETALLE_COLECCION_KOHA = "Detalle Colección Koha"


class AnalysisCoordinate(ABC):
    def __init__(self, driver, df_encuestas=None, name: str = '', description: str = ''):
        self.driver = driver
        self.df_encuestas = df_encuestas
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

    def add_coordinate(self, coordinate: AnalysisCoordinate):
        self.coordinates.append(coordinate)

    def run_analysis(self) -> pd.DataFrame:
        results = pd.DataFrame()
        for coordinate in self.coordinates:
            coordinate_results = coordinate.calculate_score(self.bibliotecas_id)
            coordinate_results['Categoría'] = coordinate.category
            coordinate_results['Coordenada'] = coordinate.name
            results = pd.concat([results, coordinate_results], ignore_index=True)
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

# Nivel de avance en la sistematización de servicios bibliotecarios
class SistemasClasificacionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Sistemas de clasificación",
                         description="Sistemas usados para la organización de las colecciones")
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'sistemas_clasificacion']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data['Puntaje'] = data['sistemas_clasificacion'].apply(
            lambda x: 1 if pd.notnull(x) and x.strip().lower() != 'no' else 0)
        return data


class NivelDetalleOrganizacionColeccionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Nivel de detalle en la organización de la colección",
                         description="Nivel de organización de la colección en la biblioteca")
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'nivel_orden_coleccion']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        nivel_orden_scores = {
            'Sin orden ni sistema de organización.': 0,
            'Agrupación básica en estanterías sin criterio específico.': 1,
            'Organización por categorías temáticas generales.': 2,
            'Secciones etiquetadas y señalizadas por temas.': 3,
            'Clasificación con códigos simples en cada libro.': 4,
            'Sistema detallado de clasificación y catálogo completo.': 5
        }
        data['Puntaje'] = data['nivel_orden_coleccion'].map(nivel_orden_scores)
        return data


class TiempoBusquedaLibroCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Tiempo que le toma hallar un libro",
                         description="Tiempo que toma encontrar un libro en la biblioteca")
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'tiempo_busqueda_libro']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        tiempo_busqueda_scores = {
            '30 minutos o más.': 0,
            '20 minutos.': 1,
            '10 minutos.': 2,
            '5 minutos.': 3
        }
        data['Puntaje'] = data['tiempo_busqueda_libro'].map(tiempo_busqueda_scores)
        return data


class SistemaRegistroUsuariosCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Sistema de registro de usuarios",
                         description="Sistema de registro de usuarios de la biblioteca")
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'sistema_registro_usuarios']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        registro_usuarios_scores = {
            'No registra.': 0,
            'Registro análogo.': 1,
            'Registro en hojas de cálculo.': 2,
            'Registro en software bibliográfico.': 3
        }
        data['Puntaje'] = data['sistema_registro_usuarios'].map(registro_usuarios_scores)
        return data


class ReglamentoServiciosCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Reglamento de servicios",
                         description="Reglamento de servicios de la biblioteca")
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'reglamento_servicios']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        reglamento_servicios_scores = {
            'No existe.': 0,
            'Reglamento en borrador.': 1,
            'Reglamento aprobado internamente.': 2,
            'Reglamento difundido a los usuarios.': 3
        }
        data['Puntaje'] = data['reglamento_servicios'].map(reglamento_servicios_scores)
        return data


class SistematizacionPrestamoExternoCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Sistematización en préstamo externo",
                         description="Nivel de sistematización en el proceso de préstamo de la biblioteca")
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'sistematizacion_prestamo_externo']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        prestamo_externo_scores = {
            'No realiza préstamos.': 0,
            'Préstamos sin registro.': 1,
            'Registro análogo.': 2,
            'Registro hoja de cálculo.': 3,
            'Registro en software bibliográfico.': 4
        }
        data['Puntaje'] = data['sistematizacion_prestamo_externo'].map(prestamo_externo_scores)
        return data


# Caracterización de la colección
class DiversidadColeccionesCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        super().__init__(driver, name="Diversidad de Colecciones",
                         description="Tipos de colección disponibles en la biblioteca")
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> pd.DataFrame:
        with self.driver.session() as session:
            result = session.run(Neo4JQueryManager.diversidad_colecciones())
            return pd.DataFrame(result.data())

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data['Puntaje'] = data['tipos_coleccion'].apply(lambda x: len(x))
        return data


class CantidadMaterialBibliograficoCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        super().__init__(driver, name="Cantidad de Material Bibliográfico",
                         description="Número total de material bibliográfico en la colección")
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> pd.DataFrame:
        with self.driver.session() as session:
            result = session.run(Neo4JQueryManager.cantidad_material_bibliografico())
            return pd.DataFrame(result.data())

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]

        def get_score(cantidad):
            if cantidad is None:
                return None
            if cantidad < 500:
                return 0
            elif 500 <= cantidad < 1000:
                return 1
            elif 1000 <= cantidad < 3000:
                return 2
            else:
                return 3

        data['Puntaje'] = data['cantidad_inventario'].apply(get_score)
        return data


class PercepcionEstadoFisicoColeccionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Percepción del estado físico de la colección",
                         description="Percepción del estado de conservación de la colección")
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'percepcion_estado_colecciones']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        percepcion_scores = {
            'La colección está en general en mal estado.': 0,
            'Una parte significativa de la colección muestra signos de deterioro.': 1,
            'La mayoría de los materiales están bien conservados, pero algunos requieren atención.': 2,
            'La colección se encuentra en excelentes condiciones.': 3
        }
        data['Puntaje'] = data['percepcion_estado_colecciones'].map(percepcion_scores)
        return data


class EnfoquesColeccionesCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Enfoques de las colecciones",
                         description="Temas en que se enfocan las colecciones")
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'especificidad_topicos']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data['num_enfoques'] = data['especificidad_topicos'].apply(lambda x: len(x.split(',')) if pd.notnull(x) else 0)
        data['Puntaje'] = data['num_enfoques'].apply(lambda x: 3 if x == 1 else (2 if x <= 3 else 1))
        return data


class ActividadesMediacionColeccionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Actividades de mediación con la colección",
                         description="Uso de la colección en actividades de mediación")
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'actividades_mediacion']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data['Puntaje'] = data['actividades_mediacion'].apply(lambda x: 1 if pd.notnull(x) and x.strip() != '' else 0)
        return data


class FrecuenciaActividadesMediacionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Frecuencia actividades de mediación con la colección",
                         description="Frecuencia de uso de la colección en actividades de mediación")
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'frecuencia_actividades_mediacion']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        frecuencia_scores = {
            'No aplica.': 0,
            'Rara vez.': 1,
            'La mayoría de las veces.': 2,
            'Siempre.': 3
        }
        data['Puntaje'] = data['frecuencia_actividades_mediacion'].map(frecuencia_scores)
        return data


class ColeccionesEspecialesCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Colecciones especiales",
                         description="Presencia de colecciones especializadas o poco comunes")
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'colecciones_especiales']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data['Puntaje'] = data['colecciones_especiales'].apply(lambda x: 1 if x.strip().lower() == 'sí' else 0)
        return data


# Facilidad de Adopción del Catálogo en Koha
class PorcentajeColeccionCatalogadaCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Porcentaje de colección catalogada",
                         description="Porcentaje de la colección que ha sido catalogada")
        self.category = AnalysisCategory.FACILIDAD_ADOPCION_KOHA.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'porcentaje_catalogado']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]

        def get_score(porcentaje):
            if porcentaje < 25:
                return 0
            elif 25 <= porcentaje <= 75:
                return 1
            else:
                return 2

        data['Puntaje'] = data['porcentaje_catalogado'].apply(get_score)
        return data


class CapacidadTecnicaPersonalCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Capacidad técnica del personal",
                         description="Nivel de experiencia del personal con sistemas digitales")
        self.category = AnalysisCategory.FACILIDAD_ADOPCION_KOHA.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'capacidad_tecnica_personal']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        capacidad_scores = {
            'Sin experiencia': 0,
            'Necesita capacitación': 1,
            'Con experiencia previa': 2
        }
        data['Puntaje'] = data['capacidad_tecnica_personal'].map(capacidad_scores)
        return data


# Impacto de Adoptar Koha
class NivelImpactoKohaCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Nivel de impacto Koha",
                         description="Evaluación del potencial impacto de adoptar Koha en la biblioteca")
        self.category = AnalysisCategory.IMPACTO_ADOPTAR_KOHA.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'nivel_impacto_adoptar_koha']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        impacto_scores = {
            'Impacto mínimo': 0,
            'Impacto bajo': 1,
            'Impacto moderado': 2,
            'Impacto alto': 3
        }
        data['Puntaje'] = data['nivel_impacto_adoptar_koha'].map(impacto_scores)
        return data


class DiversidadServiciosCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        super().__init__(driver, name="Diversidad de servicios",
                         description="Variedad de servicios y públicos atendidos por la biblioteca")
        self.category = AnalysisCategory.IMPACTO_ADOPTAR_KOHA.value

    def get_data(self) -> pd.DataFrame:
        with self.driver.session() as session:
            result = session.run(Neo4JQueryManager.diversidad_servicios())
            return pd.DataFrame(result.data())

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]

        def get_score(num_services):
            if num_services <= 2:
                return 0
            elif 3 <= num_services <= 5:
                return 1
            elif 6 <= num_services <= 8:
                return 2
            else:
                return 3

        data['num_servicios'] = data['servicios'].apply(lambda x: len(x))
        data['Puntaje'] = data['num_servicios'].apply(get_score)
        return data


class SobrecargaAdministrativaCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Sobrecarga administrativa",
                         description="Tiempo disponible para gestionar el catálogo Koha")
        self.category = AnalysisCategory.IMPACTO_ADOPTAR_KOHA.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'sobrecarga_admin_catalogo']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        sobrecarga_scores = {
            'Menos de 1 hora': 0,
            'Entre 1 y 2 horas': 1,
            'Entre 2 y 5 horas': 2,
            'Más de 5 horas': 3
        }
        data['Puntaje'] = data['sobrecarga_admin_catalogo'].map(sobrecarga_scores)
        return data


# Detalle Colección Koha
class TiposColeccionCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        super().__init__(driver, name="Tipos de colección", description="Variedad de tipos de colección disponibles")
        self.category = AnalysisCategory.DETALLE_COLECCION_KOHA.value

    def get_data(self) -> pd.DataFrame:
        with self.driver.session() as session:
            result = session.run(Neo4JQueryManager.tipos_coleccion())
            return pd.DataFrame(result.data())

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]

        def get_score(num_tipos):
            if num_tipos <= 2:
                return 0
            elif 3 <= num_tipos <= 4:
                return 1
            elif 5 <= num_tipos <= 6:
                return 2
            else:
                return 3

        data['num_tipos_coleccion'] = data['tipos_coleccion'].apply(lambda x: len(x))
        data['Puntaje'] = data['num_tipos_coleccion'].apply(get_score)
        return data


def main():
    db_config = Neo4JConfig()
    analyzer = AnalysisManager(db_config, "data/Contacto Bibliotecas - Formulario Coordenadas.csv")

    # Nivel de avance en la digitalización del catálogo
    analyzer.add_coordinate(InfraestructuraTecnologicaCoordinate(analyzer.driver))
    analyzer.add_coordinate(EstadoDigitalizacionCatalogoCoordinate(analyzer.driver, analyzer.df_encuestas))
    analyzer.add_coordinate(PorcentajeColeccionCatalogoCoordinate(analyzer.driver, analyzer.df_encuestas))
    analyzer.add_coordinate(NivelInformacionCatalogoCoordinate(analyzer.driver, analyzer.df_encuestas))

    # Nivel de avance en la sistematización de servicios bibliotecarios
    analyzer.add_coordinate(SistemasClasificacionCoordinate(analyzer.driver, analyzer.df_encuestas))
    analyzer.add_coordinate(NivelDetalleOrganizacionColeccionCoordinate(analyzer.driver, analyzer.df_encuestas))
    analyzer.add_coordinate(TiempoBusquedaLibroCoordinate(analyzer.driver, analyzer.df_encuestas))
    analyzer.add_coordinate(SistemaRegistroUsuariosCoordinate(analyzer.driver, analyzer.df_encuestas))
    analyzer.add_coordinate(ReglamentoServiciosCoordinate(analyzer.driver, analyzer.df_encuestas))
    analyzer.add_coordinate(SistematizacionPrestamoExternoCoordinate(analyzer.driver, analyzer.df_encuestas))

    # Caracterización de la colección
    analyzer.add_coordinate(DiversidadColeccionesCoordinate(analyzer.driver))
    analyzer.add_coordinate(CantidadMaterialBibliograficoCoordinate(analyzer.driver))
    analyzer.add_coordinate(PercepcionEstadoFisicoColeccionCoordinate(analyzer.driver, analyzer.df_encuestas))
    analyzer.add_coordinate(EnfoquesColeccionesCoordinate(analyzer.driver, analyzer.df_encuestas))
    analyzer.add_coordinate(ActividadesMediacionColeccionCoordinate(analyzer.driver, analyzer.df_encuestas))
    analyzer.add_coordinate(FrecuenciaActividadesMediacionCoordinate(analyzer.driver, analyzer.df_encuestas))
    analyzer.add_coordinate(ColeccionesEspecialesCoordinate(analyzer.driver, analyzer.df_encuestas))

    # Facilidad de Adopción del Catálogo en Koha
    analyzer.add_coordinate(PorcentajeColeccionCatalogadaCoordinate(analyzer.driver, analyzer.df_encuestas))
    analyzer.add_coordinate(CapacidadTecnicaPersonalCoordinate(analyzer.driver, analyzer.df_encuestas))

    # Impacto de Adoptar Koha
    analyzer.add_coordinate(NivelImpactoKohaCoordinate(analyzer.driver, analyzer.df_encuestas))
    analyzer.add_coordinate(DiversidadServiciosCoordinate(analyzer.driver))
    analyzer.add_coordinate(SobrecargaAdministrativaCoordinate(analyzer.driver, analyzer.df_encuestas))

    # Detalle Colección Koha
    analyzer.add_coordinate(TiposColeccionCoordinate(analyzer.driver))

    # Run analysis
    results = analyzer.run_analysis()

    # Save results
    analyzer.save_results(results, 'analysis_results.csv')


if __name__ == '__main__':
    main()
