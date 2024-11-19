import os
from dataclasses import dataclass
from typing import List
import pandas as pd
from neo4j import GraphDatabase

from etl.coordinates.base import AnalysisCoordinate

from etl.coordinates.digital_level_coordinates import (
    InfraestructuraTecnologicaCoordinate,
    EstadoDigitalizacionCatalogoCoordinate,
    PorcentajeColeccionCatalogoCoordinate,
    NivelInformacionCatalogoCoordinate,
)

from etl.coordinates.sistem_level_coordinate import (
    SistemasClasificacionCoordinate,
    NivelDetalleOrganizacionColeccionCoordinate,
    TiempoBusquedaLibroCoordinate,
    SistemaRegistroUsuariosCoordinate,
    ReglamentoServiciosCoordinate,
    SistematizacionPrestamoExternoCoordinate,
)

from etl.coordinates.collection_coordinate import (
    DiversidadColeccionesCoordinate,
    CantidadMaterialBibliograficoCoordinate,
    PercepcionEstadoFisicoColeccionCoordinate,
    EnfoquesColeccionesCoordinate,
    ActividadesMediacionColeccionCoordinate,
    FrecuenciaActividadesMediacionCoordinate,
    ColeccionesEspecialesCoordinate,
)

from etl.coordinates.facilidad_adopcion_coordinate import (
    PorcentajeColeccionCatalogadaCoordinate,
    CapacidadTecnicaPersonalCoordinate,
    NivelImpactoKohaCoordinate,
    DiversidadServiciosCoordinate,
    SobrecargaAdministrativaCoordinate,
    TiposColeccionCoordinate,
)


@dataclass
class Neo4JConfig:
    uri: str = os.getenv("NEO4J_URI")
    user: str = os.getenv("NEO4J_USER")
    password: str = os.getenv("NEO4J_PASSWORD")


class OperationalizationManager:
    def __init__(self, graph_db_config: Neo4JConfig, csv_path: str):
        self.driver = GraphDatabase.driver(
            graph_db_config.uri, auth=(graph_db_config.user, graph_db_config.password)
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
            coordinate_results["Categoría"] = coordinate.category
            coordinate_results["Coordenada"] = coordinate.name
            results = pd.concat([results, coordinate_results], ignore_index=True)
        return results

    @staticmethod
    def save_results(results: pd.DataFrame, output_path: str):
        results.to_csv(output_path, index=False)


def main():
    db_config = Neo4JConfig()
    analyzer = OperationalizationManager(
        db_config, "../data/Contacto Bibliotecas - Formulario Coordenadas.csv"
    )

    coordinates_config = {
        # Nivel de avance en la digitalización del catálogo
        "infraestructura": True,
        "estado_digitalizacion": True,
        "porcentaje_coleccion": True,
        "nivel_informacion": True,
        # Nivel de avance en la sistematización
        "sistemas_clasificacion": False,
        "nivel_detalle": False,
        "tiempo_busqueda": False,
        "sistema_registro": False,
        "reglamento_servicios": False,
        "sistematizacion_prestamo": False,
        # Caracterización de la colección
        "diversidad_colecciones": False,
        "cantidad_material": False,
        "percepcion_estado": False,
        "enfoques_colecciones": False,
        "actividades_mediacion": False,
        "frecuencia_actividades": False,
        "colecciones_especiales": False,
        # Facilidad de Adopción del Catálogo en Koha
        "porcentaje_catalogada": False,
        "capacidad_tecnica": False,
        # Impacto de Adoptar Koha
        "nivel_impacto": False,
        "diversidad_servicios": False,
        "sobrecarga_administrativa": False,
        # Detalle Colección Koha
        "tipos_coleccion": False,
    }

    coordinate_mappings = {
        # Nivel de avance en la digitalización del catálogo
        "infraestructura": InfraestructuraTecnologicaCoordinate(analyzer.driver),
        "estado_digitalizacion": EstadoDigitalizacionCatalogoCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        "porcentaje_coleccion": PorcentajeColeccionCatalogoCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        "nivel_informacion": NivelInformacionCatalogoCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        # Nivel de avance en la sistematización
        "sistemas_clasificacion": SistemasClasificacionCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        "nivel_detalle": NivelDetalleOrganizacionColeccionCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        "tiempo_busqueda": TiempoBusquedaLibroCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        "sistema_registro": SistemaRegistroUsuariosCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        "reglamento_servicios": ReglamentoServiciosCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        "sistematizacion_prestamo": SistematizacionPrestamoExternoCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        # Caracterización de la colección
        "diversidad_colecciones": DiversidadColeccionesCoordinate(analyzer.driver),
        "cantidad_material": CantidadMaterialBibliograficoCoordinate(analyzer.driver),
        "percepcion_estado": PercepcionEstadoFisicoColeccionCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        "enfoques_colecciones": EnfoquesColeccionesCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        "actividades_mediacion": ActividadesMediacionColeccionCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        "frecuencia_actividades": FrecuenciaActividadesMediacionCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        "colecciones_especiales": ColeccionesEspecialesCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        # Facilidad de Adopción del Catálogo en Koha
        "porcentaje_catalogada": PorcentajeColeccionCatalogadaCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        "capacidad_tecnica": CapacidadTecnicaPersonalCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        # Impacto de Adoptar Koha
        "nivel_impacto": NivelImpactoKohaCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        "diversidad_servicios": DiversidadServiciosCoordinate(analyzer.driver),
        "sobrecarga_administrativa": SobrecargaAdministrativaCoordinate(
            analyzer.driver, analyzer.df_encuestas
        ),
        # Detalle Colección Koha
        "tipos_coleccion": TiposColeccionCoordinate(analyzer.driver),
    }

    # Add only enabled coordinates
    for key, enabled in coordinates_config.items():
        if enabled:
            analyzer.add_coordinate(coordinate_mappings[key])

    # Run analysis
    results = analyzer.run_analysis()

    # Save results
    analyzer.save_results(results, "analysis_results.csv")


if __name__ == "__main__":
    main()
