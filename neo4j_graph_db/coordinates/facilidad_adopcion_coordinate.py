from neo4j_graph_db.coordinates.base import AnalysisCoordinate
from neo4j_graph_db.constants import AnalysisCategory
from neo4j_graph_db.utils import Neo4JQueryManager

from pandas import DataFrame

# Facilidad de Adopción del Catálogo en Koha
class PorcentajeColeccionCatalogadaCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Porcentaje de colección catalogada",
                         description="Porcentaje de la colección que ha sido catalogada")
        self.category = AnalysisCategory.FACILIDAD_ADOPCION_KOHA.value

    def get_data(self) -> DataFrame:
        return self.df_encuestas[['BibliotecaID', 'porcentaje_catalogado']]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
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

    def get_data(self) -> DataFrame:
        return self.df_encuestas[['BibliotecaID', 'capacidad_tecnica_personal']]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
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

    def get_data(self) -> DataFrame:
        return self.df_encuestas[['BibliotecaID', 'nivel_impacto_adoptar_koha']]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
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

    def get_data(self) -> DataFrame:
        with self.driver.session() as session:
            result = session.run(Neo4JQueryManager.diversidad_servicios())
            return DataFrame(result.data())

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
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

    def get_data(self) -> DataFrame:
        return self.df_encuestas[['BibliotecaID', 'sobrecarga_admin_catalogo']]

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
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

    def get_data(self) -> DataFrame:
        with self.driver.session() as session:
            result = session.run(Neo4JQueryManager.tipos_coleccion())
            return DataFrame(result.data())

    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
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
