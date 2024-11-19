from typing import List, Dict
from etl.core.base import DataTransformer
from etl.utils.utils import (
    parsear_fecha,
    a_float,
    a_bool,
    a_int,
)

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
            transformed_row = self._crear_objetos_neo4j(row)
            transformed_data.append(transformed_row)

        return transformed_data

    @staticmethod
    def validate_data(row: Dict) -> bool:
        """
        Validates a single data row before transformation.

        Args:
            row (Dict): Single row of data to validate

        Returns:
            bool: True if data is valid, False otherwise
        """
        required_fields = ["nombre_biblioteca", "localidad", "barrio"]
        return all(field in row and row[field] for field in required_fields)

    @staticmethod
    def crear_objetos_neo4j(fila):
        # Crear nodo BibliotecaComunitaria
        biblioteca = {
            "id": fila["2_id"],
            "nombre": fila["3_nombre_organizacion"],
            "fecha_registro": parsear_fecha(fila["1_fecha_registro"]),
            "estado": fila["4_estado"],
            "inicio_actividades": parsear_fecha(fila["20_inicio_actividades"]),
            "representante": fila["5_representante"],
            "telefono": fila["11_telefono"],
            "correo_electronico": fila["12_correo_electronico"],
            "whatsapp": fila["17_whatsapp"],
            "dias_atencion": fila["21_dias_atencion"],
            "enlace_fotos": fila["22_enlace_fotos"],
        }

        # Crear nodo Ubicacion
        ubicacion = {
            "latitud": a_float(fila["7_latitud"]),
            "longitud": a_float(fila["8_longitud"]),
            "barrio": fila["10_barrio"],
            "direccion": fila["6_direccion"],
        }

        # Crear nodo Localidad
        localidad = {"nombre": fila["9_localidad"]}

        # Crear nodo RedesSociales
        redes_sociales = {
            "facebook": fila["13_facebook"],
            "enlace_facebook": fila["14_enlace_facebook"],
            "instagram": fila["15_instagram"],
            "enlace_instagram": fila["16_enlace_instagram"],
            "youtube": fila["18_youtube"],
            "enlace_youtube": fila["19_enlace_youtube"],
        }

        # Crear nodo Coleccion
        coleccion = {
            "inventario": a_bool(fila["23_inventario"]),
            "cantidad_inventario": a_int(fila["24_cantidad_inventario"]),
            "coleccion": fila["25_coleccion"],
        }

        # Crear TiposColeccion
        tipos_coleccion = [
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
        nodos_tipos_coleccion = [
            {"nombre": tc}
            for tc in tipos_coleccion
            if a_bool(fila[f"26_tipo_coleccion_{tc}"])
        ]

        # Crear nodo Catalogo
        catalogo = {
            "catalogo": a_bool(fila["27_catalogo"]),
            "quiere_catalogo": a_bool(fila["28_quiere_catalogo"]),
        }

        # Crear nodo SoporteCatalogo
        soporte_catalogo = {"tipo": fila["29_soporte_catalogo"]}

        # Crear TiposServicio
        tipos_servicio = [
            "consulta",
            "prestamo_externo",
            "internet",
            "leo",
            "culturales",
            "alfabetizacion",
            "comunitarios",
            "otros",
        ]
        nodos_tipos_servicio = [
            {"nombre": ts} for ts in tipos_servicio if a_bool(fila[f"30_servicios_{ts}"])
        ]

        # Crear TiposActividad
        tipos_actividad = [
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
        nodos_tipos_actividad = [
            {"nombre": ta} for ta in tipos_actividad if a_bool(fila[f"31_actividades_{ta}"])
        ]

        # Crear nodo Tecnologia
        tecnologia = {"conectividad": a_bool(fila["32_conectividad"])}

        # Crear TiposTecnologia
        tipos_tecnologia = [
            "computadores",
            "impresoras",
            "tabletas",
            "proyectores",
            "smartphones",
            "ninguno",
            "otros",
        ]
        nodos_tipos_tecnologia = [
            {"nombre": tt} for tt in tipos_tecnologia if a_bool(fila[f"33_tic_{tt}"])
        ]

        # Crear TiposPoblacion
        tipos_poblacion = [
            "infancias",
            "jovenes",
            "mujeres",
            "adultos_mayores",
            "migrantes",
            "otros",
        ]
        nodos_tipos_poblacion = [
            {"nombre": tp} for tp in tipos_poblacion if a_bool(fila[f"34_poblacion_{tp}"])
        ]

        # Crear TiposAliados
        tipos_aliados = [
            "editoriales",
            "fundaciones",
            "colectivos",
            "casa_cultura",
            "consejo_cultura",
            "educativos",
            "bibliotecas_comunitarias",
            "otros",
        ]
        nodos_tipos_aliados = [
            {"nombre": ta} for ta in tipos_aliados if a_bool(fila[f"35_aliados_{ta}"])
        ]

        # Crear TiposFinanciacion
        tipos_financiacion = [
            "autogestion",
            "estimulos_distrito",
            "becas",
            "convocatorias_internacionales",
            "patrocinios",
            "otros",
        ]
        nodos_tipos_financiacion = [
            {"nombre": tf}
            for tf in tipos_financiacion
            if a_bool(fila[f"36_fuentes_financiacion_{tf}"])
        ]

        return {
            "biblioteca": biblioteca,
            "ubicacion": ubicacion,
            "localidad": localidad,
            "redes_sociales": redes_sociales,
            "coleccion": coleccion,
            "tipos_coleccion": nodos_tipos_coleccion,
            "catalogo": catalogo,
            "soporte_catalogo": soporte_catalogo,
            "tipos_servicio": nodos_tipos_servicio,
            "tipos_actividad": nodos_tipos_actividad,
            "tecnologia": tecnologia,
            "tipos_tecnologia": nodos_tipos_tecnologia,
            "tipos_poblacion": nodos_tipos_poblacion,
            "tipos_aliados": nodos_tipos_aliados,
            "tipos_financiacion": nodos_tipos_financiacion,
        }