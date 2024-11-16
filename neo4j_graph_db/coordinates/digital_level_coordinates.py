from neo4j_graph_db.coordinates.base import AnalysisCoordinate, AnalysisCategory

# Nivel de avance en la digitalización del catálogo
class InfraestructuraTecnologicaCoordinate(AnalysisCoordinate):
    def __init__(self, driver):
        super().__init__(driver, name="Infraestructura tecnológica",
                         description="Disponibilidad de internet y computador en la biblioteca")
        self.category = AnalysisCategory.CATALOGO_DIGITALIZACION.value

    def get_data(self) -> pd.DataFrame:
        with self.driver.session() as session:
            result = session.run(Neo4JQueryManager.infraestructura_tecnologica())
            return pd.DataFrame(result.data())

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data["Puntaje"] = data.apply(
            lambda row: 2 if row["tieneComputador"] and row["tieneConectividad"] else
            (1 if row["tieneComputador"] or row["tieneConectividad"] else 0), axis=1)
        return data

class EstadoDigitalizacionCatalogoCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Estado de la digitalización del catálogo",
                         description="Nivel de desarrollo en la digitalización del catálogo bibliográfico")
        self.category = AnalysisCategory.CATALOGO_DIGITALIZACION.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'tipo_catalogo']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        catalog_scores = {
            'No tiene catálogo': 0,
            'Catálogo analógico': 1,
            'Catálogo en hoja de cálculo': 2,
            'Software Bibliográfico': 3
        }
        data['Puntaje'] = data['tipo_catalogo'].map(catalog_scores)
        return data


class PorcentajeColeccionCatalogoCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="% Colección ingresada al catalogo",
                         description="Porcentaje de la colección bibliográfica que ha sido catalogada aproximadamente")
        self.category = AnalysisCategory.CATALOGO_DIGITALIZACION.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'porcentaje_coleccion_catalogada']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        data['Puntaje'] = data['porcentaje_coleccion_catalogada']
        return data


class NivelInformacionCatalogoCoordinate(AnalysisCoordinate):
    def __init__(self, driver, df_encuestas):
        super().__init__(driver, df_encuestas, name="Nivel de información capturada en el catálogo",
                         description="Detalle de la información capturada en el catálogo")
        self.category = AnalysisCategory.CATALOGO_DIGITALIZACION.value

    def get_data(self) -> pd.DataFrame:
        return self.df_encuestas[['BibliotecaID', 'nivel_detalle_catalogo']]

    def calculate_score(self, bibliotecas: List[str]) -> pd.DataFrame:
        data = self.get_data()
        data = data[data["BibliotecaID"].isin(bibliotecas)]
        detail_scores = {
            'Sin información': 0,
            'Descripción del material': 1,
            'Sistemas de Clasificación': 2,
            'Estado de disponibilidad de los materiales': 3
        }
        data['Puntaje'] = data['nivel_detalle_catalogo'].map(detail_scores)
        return data

