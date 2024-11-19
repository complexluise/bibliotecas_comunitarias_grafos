import pandas as pd

from etl.core import DataTransformer
from etl.sources.operationalization_source import OperationalizationDataSource
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
from etl.utils.utils import setup_logger

logger = setup_logger("etl.log", "etl.transformers.operationalization")


class OperationalizationTransformer(DataTransformer):
    def __init__(self):
        self.coordinates_config = {
            # Nivel de avance en la digitalización del catálogo
            "infraestructura": True,
            "estado_digitalizacion": True,
            "porcentaje_coleccion": True,
            "nivel_informacion": True,
            # Nivel de avance en la sistematización
            "sistemas_clasificacion": True,
            "nivel_detalle": True,
            "tiempo_busqueda": True,
            "sistema_registro": True,
            "reglamento_servicios": True,
            "sistematizacion_prestamo": True,
            # Caracterización de la colección
            "diversidad_colecciones": True,
            "cantidad_material": True,
            "percepcion_estado": True,
            "enfoques_colecciones": True,
            "actividades_mediacion": True,
            "frecuencia_actividades": True,
            "colecciones_especiales": True,
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

    def transform(self, data_source: OperationalizationDataSource) -> pd.DataFrame:
        logger.info("Starting operationalization transformation")
        coordinate_mappings = {
            # Nivel de avance en la digitalización del catálogo
            "infraestructura": InfraestructuraTecnologicaCoordinate(data_source.driver),
            "estado_digitalizacion": EstadoDigitalizacionCatalogoCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            "porcentaje_coleccion": PorcentajeColeccionCatalogoCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            "nivel_informacion": NivelInformacionCatalogoCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            # Nivel de avance en la sistematización
            "sistemas_clasificacion": SistemasClasificacionCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            "nivel_detalle": NivelDetalleOrganizacionColeccionCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            "tiempo_busqueda": TiempoBusquedaLibroCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            "sistema_registro": SistemaRegistroUsuariosCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            "reglamento_servicios": ReglamentoServiciosCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            "sistematizacion_prestamo": SistematizacionPrestamoExternoCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            # Caracterización de la colección
            "diversidad_colecciones": DiversidadColeccionesCoordinate(
                data_source.driver
            ),
            "cantidad_material": CantidadMaterialBibliograficoCoordinate(
                data_source.driver
            ),
            "percepcion_estado": PercepcionEstadoFisicoColeccionCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            "enfoques_colecciones": EnfoquesColeccionesCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            "actividades_mediacion": ActividadesMediacionColeccionCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            "frecuencia_actividades": FrecuenciaActividadesMediacionCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            "colecciones_especiales": ColeccionesEspecialesCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            # Facilidad de Adopción del Catálogo en Koha
            "porcentaje_catalogada": PorcentajeColeccionCatalogadaCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            "capacidad_tecnica": CapacidadTecnicaPersonalCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            # Impacto de Adoptar Koha
            "nivel_impacto": NivelImpactoKohaCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            "diversidad_servicios": DiversidadServiciosCoordinate(data_source.driver),
            "sobrecarga_administrativa": SobrecargaAdministrativaCoordinate(
                data_source.driver, data_source.df_encuestas
            ),
            # Detalle Colección Koha
            "tipos_coleccion": TiposColeccionCoordinate(data_source.driver),
        }

        results = pd.DataFrame({"BibliotecaID": data_source.bibliotecas_id})
        for key, enabled in self.coordinates_config.items():
            if enabled:
                logger.debug(f"Processing coordinate: {key}")
                coordinate = coordinate_mappings[key]
                try:
                    coordinate_results = coordinate.calculate_score(
                        data_source.bibliotecas_id
                    )
                    results = results.merge(
                        coordinate_results, on="BibliotecaID", how="left"
                    )
                    logger.info(f"Successfully processed coordinate: {key}")
                except Exception as e:
                    logger.error(f"Error processing coordinate {key}: {str(e)}")
                    raise

            logger.info(f"Transformation completed. Generated {len(results)} results")
        return results
