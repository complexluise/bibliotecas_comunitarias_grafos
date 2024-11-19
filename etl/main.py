from etl.utils.utils import extract_csv, setup_logger
from etl.neo4j_import_db_SiBiBo import crear_objetos_neo4j, cargar_datos_en_neo4j
from etl.utils.models import Neo4JConfig


registrador = setup_logger("etl_main.log")
config = Neo4JConfig()

class ProcesoETL:
    """
    Clase que maneja el proceso ETL para datos de bibliotecas.

    Esta clase implementa las operaciones de Extracción, Transformación y Carga
    de datos para su procesamiento en Neo4j.

    Atributos:
        configuracion_neo4j (Neo4JConfig): Configuración para la conexión a Neo4j.
        datos_crudos (dict): Almacena los datos sin procesar.
        datos_transformados (dict): Almacena los datos después de la transformación.
    """

    def __init__(self, configuracion_neo4j: Neo4JConfig):
        self.configuracion_neo4j = configuracion_neo4j
        self.datos_crudos = {}
        self.datos_transformados = {}

    @staticmethod
    def extraer(ruta_archivo_csv: str) -> list[dict]:
        """
        Extrae datos desde un archivo CSV.

        Args:
            ruta_archivo_csv (str): Ruta al archivo CSV que contiene los datos.

        Returns:
            list[dict]: Lista de diccionarios con los datos extraídos del CSV.
        """
        registrador.info("Iniciando extracción de datos")
        return extract_csv(ruta_archivo_csv)

    @staticmethod
    def transformar(datos_crudos: list[dict]) -> list[dict]:
        """
        Transforma los datos crudos al formato compatible con Neo4j.

        Args:
            datos_crudos (list[dict]): Lista de diccionarios con los datos sin procesar.

        Returns:
            list[dict]: Lista de diccionarios con los datos transformados.
        """
        registrador.info("Iniciando transformación de datos")
        return [crear_objetos_neo4j(fila) for fila in datos_crudos]

    def cargar(self, datos_transformados: list[dict]):
        """
        Carga los datos transformados en la base de datos Neo4j.

        Args:
            datos_transformados (list[dict]): Lista de diccionarios con los datos
                                            transformados listos para cargar.
        """
        registrador.info("Iniciando carga de datos en Neo4j")
        cargar_datos_en_neo4j(
            uri=self.configuracion_neo4j.uri,
            usuario=self.configuracion_neo4j.user,
            contraseña=self.configuracion_neo4j.password,
            datos=datos_transformados
        )


def principal():
    """
    Función principal que ejecuta el proceso ETL completo.

    Coordina la extracción de datos desde un CSV, su transformación
    y posterior carga en Neo4j.
    """
    ruta_archivo_csv = '../data/BASE DE DATOS DE BIBLIOTECAS COMUNITARIAS DE BOGOTÁ - SIBIBO 2024 - Base de datos.csv'

    etl = ProcesoETL(config)

    try:
        # Extraer
        datos_crudos = etl.extraer(ruta_archivo_csv)
        registrador.info(f"Extraídos {len(datos_crudos)} registros")

        # Transformar
        datos_transformados = etl.transformar(datos_crudos)
        registrador.info(f"Transformados {len(datos_transformados)} registros")

        # Cargar
        etl.cargar(datos_transformados)
        registrador.info("Proceso ETL completado exitosamente")

    except Exception as e:
        registrador.error(f"Proceso ETL fallido: {str(e)}")
        raise


if __name__ == "__main__":
    principal()
