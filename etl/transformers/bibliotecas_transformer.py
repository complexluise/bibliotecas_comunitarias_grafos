from typing import List, Dict
from dataclasses import dataclass
from logging import getLogger
from etl.core.base import DataTransformer
from etl.utils.utils import parsear_fecha, a_float, a_bool, a_int

logger = getLogger(__name__)


@dataclass
class TransformationConfig:
    TIPOS_COLECCION = [
        "literatura",
        "infantiles",
        "informativos",
        "texto",
        "didacticos",
        "revistas_periodicos",
        "audiovisuales",
        "juegos",
        "digitales",
        "fanzines",
        "enfoques",
        "autoedicion",
        "otros",
    ]
    TIPOS_SERVICIO = [
        "consulta",
        "prestamo_externo",
        "internet",
        "leo",
        "culturales",
        "alfabetizacion",
        "comunitarios",
        "otros",
    ]
    TIPOS_ACTIVIDAD = [
        "lectoescritura",
        "culturales",
        "formacion",
        "emprendimientos",
        "produccion_comunitaria",
        "medioambientales",
        "psicosociales",
        "ciencia",
        "otros",
    ]
    TIPOS_TECNOLOGIA = [
        "computadores",
        "impresoras",
        "tabletas",
        "proyectores",
        "smartphones",
        "ninguno",
        "otros",
    ]
    TIPOS_POBLACION = [
        "infancias",
        "jovenes",
        "mujeres",
        "adultos_mayores",
        "migrantes",
        "otros",
    ]
    TIPOS_ALIADOS = [
        "editoriales",
        "fundaciones",
        "colectivos",
        "casa_cultura",
        "consejo_cultura",
        "educativos",
        "bibliotecas_comunitarias",
        "otros",
    ]
    TIPOS_FINANCIACION = [
        "autogestion",
        "estimulos_distrito",
        "becas",
        "convocatorias_internacionales",
        "patrocinios",
        "otros",
    ]


class BibliotecasTransformer(DataTransformer):
    """
    Transformer for Bibliotecas Comunitarias data.
    Handles the specific transformation logic for library data before loading into Neo4j.
    """

    def transform(self, data: List[Dict]) -> List[Dict]:
        """
        Transforms raw library data into Neo4j compatible format.

        Args:
            data (List[Dict]): Raw data from the source

        Returns:
            List[Dict]: Transformed data ready for Neo4j import
        """
        transformed_data = []

        for row in data:
            try:
                if self.validate_data(row):
                    transformed_row = self._transform_row(row)
                    transformed_data.append(transformed_row)
                else:
                    logger.warning(
                        f"Invalid data row: {row.get('nombre_biblioteca', 'unknown')}"
                    )
            except Exception as e:
                logger.error(f"Error transforming row: {e}")
                continue

        return transformed_data

    def _transform_row(self, row: Dict) -> Dict:
        """Transform a single data row into Neo4j format."""
        return {
            "biblioteca": self._transform_biblioteca(row),
            "ubicacion": self._transform_ubicacion(row),
            "localidad": {"nombre": row["9_localidad"]},
            "redes_sociales": self._transform_redes_sociales(row),
            "coleccion": self._transform_coleccion(row),
            "tipos_coleccion": self._transform_tipos(
                row, TransformationConfig.TIPOS_COLECCION, "26_tipo_coleccion_"
            ),
            "catalogo": self._transform_catalogo(row),
            "soporte_catalogo": {"tipo": row["29_soporte_catalogo"]},
            "tipos_servicio": self._transform_tipos(
                row, TransformationConfig.TIPOS_SERVICIO, "30_servicios_"
            ),
            "tipos_actividad": self._transform_tipos(
                row, TransformationConfig.TIPOS_ACTIVIDAD, "31_actividades_"
            ),
            "tecnologia": {"conectividad": a_bool(row["32_conectividad"])},
            "tipos_tecnologia": self._transform_tipos(
                row, TransformationConfig.TIPOS_TECNOLOGIA, "33_tic_"
            ),
            "tipos_poblacion": self._transform_tipos(
                row, TransformationConfig.TIPOS_POBLACION, "34_poblacion_"
            ),
            "tipos_aliados": self._transform_tipos(
                row, TransformationConfig.TIPOS_ALIADOS, "35_aliados_"
            ),
            "tipos_financiacion": self._transform_tipos(
                row, TransformationConfig.TIPOS_FINANCIACION, "36_fuentes_financiacion_"
            ),
        }

    @staticmethod
    def _transform_biblioteca(row: Dict) -> Dict:
        """Transform biblioteca-specific fields."""
        return {
            "id": row["2_id"],
            "nombre": row["3_nombre_organizacion"],
            "fecha_registro": parsear_fecha(row["1_fecha_registro"]),
            "estado": row["4_estado"],
            "inicio_actividades": parsear_fecha(row["20_inicio_actividades"]),
            "representante": row["5_representante"],
            "telefono": row["11_telefono"],
            "correo_electronico": row["12_correo_electronico"],
            "whatsapp": row["17_whatsapp"],
            "dias_atencion": row["21_dias_atencion"],
            "enlace_fotos": row["22_enlace_fotos"],
        }

    @staticmethod
    def _transform_ubicacion(row: Dict) -> Dict:
        """Transform location-specific fields."""
        return {
            "latitud": a_float(row["7_latitud"]),
            "longitud": a_float(row["8_longitud"]),
            "barrio": row["10_barrio"],
            "direccion": row["6_direccion"],
        }

    @staticmethod
    def _transform_redes_sociales(row: Dict) -> Dict:
        """Transform social media fields."""
        return {
            "facebook": row["13_facebook"],
            "enlace_facebook": row["14_enlace_facebook"],
            "instagram": row["15_instagram"],
            "enlace_instagram": row["16_enlace_instagram"],
            "youtube": row["18_youtube"],
            "enlace_youtube": row["19_enlace_youtube"],
        }

    @staticmethod
    def _transform_coleccion(row: Dict) -> Dict:
        """Transform collection-specific fields."""
        return {
            "inventario": a_bool(row["23_inventario"]),
            "cantidad_inventario": a_int(row["24_cantidad_inventario"]),
            "coleccion": row["25_coleccion"],
        }

    @staticmethod
    def _transform_catalogo(row: Dict) -> Dict:
        """Transform catalog-specific fields."""
        return {
            "catalogo": a_bool(row["27_catalogo"]),
            "quiere_catalogo": a_bool(row["28_quiere_catalogo"]),
        }

    @staticmethod
    def _transform_tipos(row: Dict, tipos: List[str], prefix: str) -> List[Dict]:
        """Transform type-specific fields with common prefix."""
        return [
            {"nombre": tipo} for tipo in tipos if a_bool(row.get(f"{prefix}{tipo}"))
        ]

    @staticmethod
    def validate_data(row: Dict) -> bool:
        """
        Validates a single data row before transformation.

        Args:
            row (Dict): Single row of data to validate

        Returns:
            bool: True if data is valid, False otherwise
        """
        required_fields = ["2_id", "3_nombre_organizacion", "9_localidad", "10_barrio"]
        return all(field in row and row[field] for field in required_fields)
