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
            column_name="sistemas_clasificacion",
            description="Sistemas usados para la organización de las colecciones",
        )
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data[self.column_name] = data[self.column_name].apply(
            lambda x: 1 if notnull(x) and x.strip().lower() != "no" else 0
        )
        return data


class NivelDetalleOrganizacionColeccionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Nivel de detalle en la organización de la colección",
            column_name="nivel_detalle_organizacion_coleccion",
            description="Nivel de organización de la colección en la biblioteca",
        )
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

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
        data[self.column_name] = data[self.column_name].map(nivel_orden_scores)
        return data


class TiempoBusquedaLibroCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Tiempo que le toma hallar un libro",
            column_name="tiempo_busqueda_libro",
            description="Tiempo que toma encontrar un libro en la biblioteca",
        )
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        tiempo_busqueda_scores = {
            "30 minutos o más.": 0,
            "20 minutos.": 1,
            "10 minutos.": 2,
            "5 minutos.": 3,
        }
        data[self.column_name] = data[self.column_name].map(tiempo_busqueda_scores)
        return data


class SistemaRegistroUsuariosCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Sistema de registro de usuarios",
            column_name="sistema_registro_usuarios",
            description="Sistema de registro de usuarios de la biblioteca",
        )
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        registro_usuarios_scores = {
            "No registra.": 0,
            "Registro análogo.": 1,
            "Registro en hojas de cálculo.": 2,
            "Registro en software bibliográfico.": 3,
        }
        data[self.column_name] = data[self.column_name].map(registro_usuarios_scores)
        return data


class ReglamentoServiciosCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Reglamento de servicios",
            column_name="reglamento_servicios",
            description="Reglamento de servicios de la biblioteca",
        )
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        reglamento_servicios_scores = {
            "No existe.": 0,
            "Reglamento en borrador.": 1,
            "Reglamento aprobado internamente.": 2,
            "Reglamento difundido a los usuarios.": 3,
        }
        data[self.column_name] = data[self.column_name].map(reglamento_servicios_scores)
        return data


class SistematizacionPrestamoExternoCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Sistematización en préstamo externo",
            column_name="sistematizacion_prestamo_externo",
            description="Nivel de sistematización en el proceso de préstamo de la biblioteca",
        )
        self.category = AnalysisCategory.SISTEMATIZACION_SERVICIOS.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

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
        data[self.column_name] = data[self.column_name].map(prestamo_externo_scores)
        return data
