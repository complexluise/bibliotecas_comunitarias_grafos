from etl.coordinates.base import AnalysisCoordinate
from etl.utils.constants import AnalysisCategory
from etl.graph_db.query_manager import Neo4JQueryManager
from pandas import DataFrame, notnull


class DiversidadColeccionesCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        super().__init__(
            driver,
            name="Diversidad de Colecciones",
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
        data["Puntaje"] = data["tipos_coleccion"].apply(lambda x: len(x))
        return data


class CantidadMaterialBibliograficoCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        super().__init__(
            driver,
            name="Cantidad de Material Bibliográfico",
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

        data["Puntaje"] = data["cantidad_inventario"].apply(get_score)
        return data


class PercepcionEstadoFisicoColeccionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Percepción del estado físico de la colección",
            description="Percepción del estado de conservación de la colección",
        )
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", "percepcion_estado_colecciones"]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        percepcion_scores = {
            "La colección está en general en mal estado.": 0,
            "Una parte significativa de la colección muestra signos de deterioro.": 1,
            "La mayoría de los materiales están bien conservados, pero algunos requieren atención.": 2,
            "La colección se encuentra en excelentes condiciones.": 3,
        }
        data["Puntaje"] = data["percepcion_estado_colecciones"].map(percepcion_scores)
        return data


class EnfoquesColeccionesCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Enfoques de las colecciones",
            description="Temas en que se enfocan las colecciones",
        )
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", "especificidad_topicos"]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data["num_enfoques"] = data["especificidad_topicos"].apply(
            lambda x: len(x.split(",")) if notnull(x) else 0
        )
        data["Puntaje"] = data["num_enfoques"].apply(
            lambda x: 3 if x == 1 else (2 if x <= 3 else 1)
        )
        return data


class ActividadesMediacionColeccionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Actividades de mediación con la colección",
            description="Uso de la colección en actividades de mediación",
        )
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", "actividades_mediacion"]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data["Puntaje"] = data["actividades_mediacion"].apply(
            lambda x: 1 if notnull(x) and x.strip() != "" else 0
        )
        return data


class FrecuenciaActividadesMediacionCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Frecuencia actividades de mediación con la colección",
            description="Frecuencia de uso de la colección en actividades de mediación",
        )
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", "frecuencia_actividades_mediacion"]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        frecuencia_scores = {
            "No aplica.": 0,
            "Rara vez.": 1,
            "La mayoría de las veces.": 2,
            "Siempre.": 3,
        }
        data["Puntaje"] = data["frecuencia_actividades_mediacion"].map(
            frecuencia_scores
        )
        return data


class ColeccionesEspecialesCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(
            driver,
            df_encuestas,
            name="Colecciones especiales",
            description="Presencia de colecciones especializadas o poco comunes",
        )
        self.category = AnalysisCategory.COLECCION_CARACTERIZACION.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[["BibliotecaID", "colecciones_especiales"]]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data["Puntaje"] = data["colecciones_especiales"].apply(
            lambda x: 1 if x.strip().lower() == "sí" else 0
        )
        return data
