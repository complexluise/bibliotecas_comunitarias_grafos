from etl.coordinates.base import AnalysisCoordinate
from etl.utils.constants import AnalysisCategory
from etl.utils.query_manager import Neo4JQueryManager
from pandas import DataFrame, notnull


class DiversidadColeccionesCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        super().__init__(
            driver,
            name="Diversidad de Colecciones",
            column_name="diversidad_colecciones",
            description="Tipos de colección disponibles en la biblioteca",
        )
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> DataFrame:
        with self.driver.session() as session:
            result = session.run(Neo4JQueryManager.diversidad_colecciones())
            return DataFrame(result.data())

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data[self.column_name] = data["tipos_coleccion"].apply(lambda x: len(x))
        return data


class CantidadMaterialBibliograficoCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        super().__init__(
            driver,
            name="Cantidad de Material Bibliográfico",
            column_name="cantidad_material_bibliografico",
            description="Número total de material bibliográfico en la colección",
        )
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> DataFrame:
        with self.driver.session() as session:
            result = session.run(Neo4JQueryManager.cantidad_material_bibliografico())
            return DataFrame(result.data())

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
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

        data[self.column_name] = data["cantidad_inventario"].apply(get_score)
        return data


class PercepcionEstadoFisicoColeccionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Percepción del estado físico de la colección",
            column_name="percepcion_estado_colecciones",
            description="Percepción del estado de conservación de la colección",
        )
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        percepcion_scores = {
            "La colección está en general en mal estado.": 0,
            "Una parte significativa de la colección muestra signos de deterioro.": 1,
            "La mayoría de los materiales están bien conservados, pero algunos requieren atención.": 2,
            "La colección se encuentra en excelentes condiciones.": 3,
        }
        data[self.column_name] = data[self.column_name].map(percepcion_scores)
        return data


class EnfoquesColeccionesCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Enfoques de las colecciones",
            column_name="enfoques_colecciones",
            description="Temas en que se enfocan las colecciones",
        )
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data["num_enfoques"] = data[self.column_name].apply(
            lambda x: len(x.split(",")) if notnull(x) else 0
        )
        data[self.column_name] = data["num_enfoques"].apply(
            lambda x: 3 if x == 1 else (2 if x <= 3 else 1)
        )
        return data


class ActividadesMediacionColeccionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Actividades de mediación con la colección",
            column_name="actividades_mediacion",
            description="Uso de la colección en actividades de mediación",
        )
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data[self.column_name] = data[self.column_name].apply(
            lambda x: 1 if notnull(x) and x.strip() != "" else 0
        )
        return data


class FrecuenciaActividadesMediacionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Frecuencia actividades de mediación con la colección",
            column_name="frecuencia_actividades_mediacion",
            description="Frecuencia de uso de la colección en actividades de mediación",
        )
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        frecuencia_scores = {
            "No aplica.": 0,
            "Rara vez.": 1,
            "La mayoría de las veces.": 2,
            "Siempre.": 3,
        }
        data[self.column_name] = data[self.column_name].map(frecuencia_scores)
        return data


class ColeccionesEspecialesCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Colecciones especiales",
            column_name="colecciones_especiales",
            description="Presencia de colecciones especializadas o poco comunes",
        )
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", self.column_name]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data[self.column_name] = data[self.column_name].apply(
            lambda x: 1 if x.strip().lower() == "sí" else 0
        )
        return data
