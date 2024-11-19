from etl.coordinates.base import AnalysisCoordinate
from etl.utils.constants import AnalysisCategory
from etl.utils.query_manager import Neo4JQueryManager

from pandas import DataFrame


# Nivel de avance en la digitalización del catálogo
class InfraestructuraTecnologicaCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        super().__init__(
            driver,
            name="Infraestructura tecnológica",
            column_name="infraestructura_tecnologica",
            description="Disponibilidad de internet y computador en la biblioteca",
        )
        self.category = AnalysisCategory.CATALOGO_DIGITALIZACION.value

    def get_data(self) -> DataFrame:
        with self.driver.session() as session:
            result = session.run(Neo4JQueryManager.infraestructura_tecnologica())
            return DataFrame(result.data())

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data[self.column_name] = data.apply(
            lambda row: (
                2
                if row["tieneComputador"] and row["tieneConectividad"]
                else (1 if row["tieneComputador"] or row["tieneConectividad"] else 0)
            ),
            axis=1,
        )
        return data


class EstadoDigitalizacionCatalogoCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Estado de la digitalización del catálogo",
            column_name="catalogo_digitalización",
            description="Nivel de desarrollo en la digitalización del catálogo bibliográfico",
        )
        self.category = AnalysisCategory.CATALOGO_DIGITALIZACION.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        catalog_scores = {
            "No tiene catálogo.": 0,
            "Catálogo analógico.": 1,
            "Catálogo en hoja de cálculo.": 2,
            "Software Bibliográfico.": 3,
        }
        data[self.column_name] = data[self.column_name].map(catalog_scores)
        return data


class PorcentajeColeccionCatalogoCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="% Colección ingresada al catalogo",
            column_name="porcentaje_coleccion_catalogada",
            description="Porcentaje de la colección bibliográfica que ha sido catalogada aproximadamente",
        )
        self.category = AnalysisCategory.CATALOGO_DIGITALIZACION.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data[self.column_name] = data[self.column_name]
        return data


class NivelInformacionCatalogoCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Nivel de información capturada en el catálogo",
            column_name="nivel_detalle_catalogo",
            description="Detalle de la información capturada en el catálogo",
        )
        self.category = AnalysisCategory.CATALOGO_DIGITALIZACION.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        detail_scores = {
            "Sin información": 0,
            "Descripción del material": 1,
            "Sistemas de Clásificación (Incluye anteriores)": 2,
            "Identificadores como ISBN (Incluye anterior)": 3,
            "Estado de disponibilidad de los materiales (Incluye anterior)": 4,
        }
        data[self.column_name] = data[self.column_name].map(detail_scores)
        return data
