from etl.coordinates.base import AnalysisCoordinate
from etl.utils.constants import AnalysisCategory

from pandas import DataFrame, notnull


# Nivel de avance en la sistematización de servicios bibliotecarios
class SistemasClasificacionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Sistemas de clasificación",
            description="Sistemas usados para la organización de las colecciones",
        )
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", "sistemas_clasificacion"]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data["Puntaje"] = data["sistemas_clasificacion"].apply(
            lambda x: 1 if notnull(x) and x.strip().lower() != "no" else 0
        )
        return data


class NivelDetalleOrganizacionColeccionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Nivel de detalle en la organización de la colección",
            description="Nivel de organización de la colección en la biblioteca",
        )
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", "nivel_orden_coleccion"]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        nivel_orden_scores = {
            "Sin orden ni sistema de organización.": 0,
            "Agrupación básica en estanterías sin criterio específico.": 1,
            "Organización por categorías temáticas generales.": 2,
            "Secciones etiquetadas y señalizadas por temas.": 3,
            "Clasificación con códigos simples en cada libro.": 4,
            "Sistema detallado de clasificación y catálogo completo.": 5,
        }
        data["Puntaje"] = data["nivel_orden_coleccion"].map(nivel_orden_scores)
        return data


class TiempoBusquedaLibroCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Tiempo que le toma hallar un libro",
            description="Tiempo que toma encontrar un libro en la biblioteca",
        )
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", "tiempo_busqueda_libro"]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        tiempo_busqueda_scores = {
            "30 minutos o más.": 0,
            "20 minutos.": 1,
            "10 minutos.": 2,
            "5 minutos.": 3,
        }
        data["Puntaje"] = data["tiempo_busqueda_libro"].map(tiempo_busqueda_scores)
        return data


class SistemaRegistroUsuariosCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Sistema de registro de usuarios",
            description="Sistema de registro de usuarios de la biblioteca",
        )
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", "sistema_registro_usuarios"]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        registro_usuarios_scores = {
            "No registra.": 0,
            "Registro análogo.": 1,
            "Registro en hojas de cálculo.": 2,
            "Registro en software bibliográfico.": 3,
        }
        data["Puntaje"] = data["sistema_registro_usuarios"].map(
            registro_usuarios_scores
        )
        return data


class ReglamentoServiciosCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Reglamento de servicios",
            description="Reglamento de servicios de la biblioteca",
        )
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", "reglamento_servicios"]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        reglamento_servicios_scores = {
            "No existe.": 0,
            "Reglamento en borrador.": 1,
            "Reglamento aprobado internamente.": 2,
            "Reglamento difundido a los usuarios.": 3,
        }
        data["Puntaje"] = data["reglamento_servicios"].map(reglamento_servicios_scores)
        return data


class SistematizacionPrestamoExternoCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Sistematización en préstamo externo",
            description="Nivel de sistematización en el proceso de préstamo de la biblioteca",
        )
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", "sistematizacion_prestamo_externo"]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        prestamo_externo_scores = {
            "No realiza préstamos.": 0,
            "Préstamos sin registro.": 1,
            "Registro análogo.": 2,
            "Registro hoja de cálculo.": 3,
            "Registro en software bibliográfico.": 4,
        }
        data["Puntaje"] = data["sistematizacion_prestamo_externo"].map(
            prestamo_externo_scores
        )
        return data
