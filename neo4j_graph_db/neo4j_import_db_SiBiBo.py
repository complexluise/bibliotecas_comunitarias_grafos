import csv
from neo4j import GraphDatabase
from datetime import datetime

from utils import Neo4jQueryManager


def extract_csv(ruta_archivo):
    with open(ruta_archivo, 'r', encoding='utf-8') as archivo:
        lector = csv.DictReader(archivo)
        return list(lector)


def parsear_fecha(cadena_fecha):
    try:
        return datetime.strptime(cadena_fecha, '%d/%m/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return None


def a_bool(valor):
    return valor.lower() in ('sí', 'si', 'yes', 'true', '1')


def a_float(valor):
    try:
        return float(valor)
    except ValueError:
        return None


def a_int(valor):
    try:
        return int(valor)
    except ValueError:
        return None


def crear_objetos_neo4j(fila):
    # Crear nodo BibliotecaComunitaria
    biblioteca = {
        "id": fila['2_id'],
        "nombre": fila['3_nombre_organizacion'],
        "fecha_registro": parsear_fecha(fila['1_fecha_registro']),
        "estado": fila['4_estado'],
        "inicio_actividades": parsear_fecha(fila['20_inicio_actividades']),
        "representante": fila['5_representante'],
        "telefono": fila['11_telefono'],
        "correo_electronico": fila['12_correo_electronico'],
        "whatsapp": fila['17_whatsapp'],
        "dias_atencion": fila['21_dias_atencion'],
        "enlace_fotos": fila['22_enlace_fotos']
    }

    # Crear nodo Ubicacion
    ubicacion = {
        "latitud": a_float(fila['7_latitud']),
        "longitud": a_float(fila['8_longitud']),
        "barrio": fila['10_barrio'],
        "direccion": fila['6_direccion']
    }

    # Crear nodo Localidad
    localidad = {
        "nombre": fila['9_localidad']
    }

    # Crear nodo RedesSociales
    redes_sociales = {
        "facebook": fila['13_facebook'],
        "enlace_facebook": fila['14_enlace_facebook'],
        "instagram": fila['15_instagram'],
        "enlace_instagram": fila['16_enlace_instagram'],
        "youtube": fila['18_youtube'],
        "enlace_youtube": fila['19_enlace_youtube']
    }

    # Crear nodo Coleccion
    coleccion = {
        "inventario": a_bool(fila['23_inventario']),
        "cantidad_inventario": a_int(fila['24_cantidad_inventario']),
        "coleccion": fila['25_coleccion']
    }

    # Crear TiposColeccion
    tipos_coleccion = [
        "literatura", "infantiles", "informativos", "texto", "didacticos",
        "revistas_periodicos", "audiovisuales", "juegos", "digitales",
        "fanzines", "enfoques", "autoedicion", "otros"
    ]
    nodos_tipos_coleccion = [
        {"nombre": tc} for tc in tipos_coleccion if a_bool(fila[f'26_tipo_coleccion_{tc}'])
    ]

    # Crear nodo Catalogo
    catalogo = {
        "catalogo": a_bool(fila['27_catalogo']),
        "quiere_catalogo": a_bool(fila['28_quiere_catalogo']),
    }

    # Crear nodo SoporteCatalogo
    soporte_catalogo = {
        "tipo": fila['29_soporte_catalogo']
    }

    # Crear TiposServicio
    tipos_servicio = [
        "consulta", "prestamo_externo", "internet", "leo", "culturales",
        "alfabetizacion", "comunitarios", "otros"
    ]
    nodos_tipos_servicio = [
        {"nombre": ts} for ts in tipos_servicio if a_bool(fila[f'30_servicios_{ts}'])
    ]

    # Crear TiposActividad
    tipos_actividad = [
        "lectoescritura", "culturales", "formacion", "emprendimientos",
        "produccion_comunitaria", "medioambientales", "psicosociales", "ciencia", "otros"
    ]
    nodos_tipos_actividad = [
        {"nombre": ta} for ta in tipos_actividad if a_bool(fila[f'31_actividades_{ta}'])
    ]

    # Crear nodo Tecnologia
    tecnologia = {
        "conectividad": a_bool(fila['32_conectividad'])
    }

    # Crear TiposTecnologia
    tipos_tecnologia = [
        "computadores", "impresoras", "tabletas", "proyectores",
        "smartphones", "ninguno", "otros"
    ]
    nodos_tipos_tecnologia = [
        {"nombre": tt} for tt in tipos_tecnologia if a_bool(fila[f'33_tic_{tt}'])
    ]

    # Crear TiposPoblacion
    tipos_poblacion = [
        "infancias", "jovenes", "mujeres", "adultos_mayores", "migrantes", "otros"
    ]
    nodos_tipos_poblacion = [
        {"nombre": tp} for tp in tipos_poblacion if a_bool(fila[f'34_poblacion_{tp}'])
    ]

    # Crear TiposAliados
    tipos_aliados = [
        "editoriales", "fundaciones", "colectivos", "casa_cultura",
        "consejo_cultura", "educativos", "bibliotecas_comunitarias", "otros"
    ]
    nodos_tipos_aliados = [
        {"nombre": ta} for ta in tipos_aliados if a_bool(fila[f'35_aliados_{ta}'])
    ]

    # Crear TiposFinanciacion
    tipos_financiacion = [
        "autogestion", "estimulos_distrito", "becas",
        "convocatorias_internacionales", "patrocinios", "otros"
    ]
    nodos_tipos_financiacion = [
        {"nombre": tf} for tf in tipos_financiacion if a_bool(fila[f'36_fuentes_financiacion_{tf}'])
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
        "tipos_financiacion": nodos_tipos_financiacion
    }


def cargar_datos_en_neo4j(uri, usuario, contraseña, datos):
    driver = GraphDatabase.driver(uri, auth=(usuario, contraseña))

    def crear_grafo(tx, datos_biblioteca):
        # Crear nodo BibliotecaComunitaria
        tx.run(
            Neo4jQueryManager.create_biblioteca_comunitaria(),
            props_biblioteca=datos_biblioteca["biblioteca"]
        )

        # Crear y vincular nodo Ubicacion
        tx.run(
            Neo4jQueryManager.create_and_link_ubicacion(),
            id_biblioteca=datos_biblioteca["biblioteca"]["id"],
            props_ubicacion=datos_biblioteca["ubicacion"]
        )

        # Crear y vincular nodo Localidad
        tx.run(
            Neo4jQueryManager.create_and_link_localidad(),
            id_biblioteca=datos_biblioteca["biblioteca"]["id"],
            nombre_localidad=datos_biblioteca["localidad"]["nombre"]
        )

        # Crear y vincular nodo RedesSociales
        tx.run(
            Neo4jQueryManager.create_and_link_redes_sociales(),
            id_biblioteca=datos_biblioteca["biblioteca"]["id"],
            props_redes_sociales=datos_biblioteca["redes_sociales"]
        )

        # Crear y vincular nodo Coleccion
        tx.run(
            Neo4jQueryManager.create_and_link_coleccion(),
            id_biblioteca=datos_biblioteca["biblioteca"]["id"],
            props_coleccion=datos_biblioteca["coleccion"]
        )

        # Crear y vincular nodos TipoColeccion
        for tc in datos_biblioteca["tipos_coleccion"]:
            tx.run(
                Neo4jQueryManager.create_and_link_tipo_coleccion(),
                id_biblioteca=datos_biblioteca["biblioteca"]["id"],
                nombre_tipo=tc["nombre"]
            )

        # Crear y vincular nodo Catalogo
        tx.run(
            Neo4jQueryManager.create_and_link_catalogo(),
            id_biblioteca=datos_biblioteca["biblioteca"]["id"],
            props_catalogo=datos_biblioteca["catalogo"]
        )

        # Crear y vincular nodo SoporteCatalogo
        tx.run(
            Neo4jQueryManager.create_and_link_soporte_catalogo(),
            id_biblioteca=datos_biblioteca["biblioteca"]["id"],
            props_soporte_catalogo=datos_biblioteca["soporte_catalogo"]
        )

        # Crear y vincular nodos TipoServicio
        for ts in datos_biblioteca["tipos_servicio"]:
            tx.run(
                Neo4jQueryManager.create_and_link_tipo_servicio(),
                id_biblioteca=datos_biblioteca["biblioteca"]["id"],
                nombre_tipo=ts["nombre"]
            )

        # Crear y vincular nodos TipoActividad
        for ta in datos_biblioteca["tipos_actividad"]:
            tx.run(
                Neo4jQueryManager.create_and_link_tipo_actividad(),
                id_biblioteca=datos_biblioteca["biblioteca"]["id"],
                nombre_tipo=ta["nombre"]
            )

        # Crear y vincular nodo Tecnologia
        tx.run("""
            MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
            CREATE (t:Tecnologia)
            SET t += $props_tecnologia
            CREATE (b)-[:USA_TECNOLOGIA]->(t)
        """, id_biblioteca=datos_biblioteca["biblioteca"]["id"], props_tecnologia=datos_biblioteca["tecnologia"])

        # Crear y vincular nodos TipoTecnologia
        for tt in datos_biblioteca["tipos_tecnologia"]:
            tx.run("""
                MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
                MERGE (tt:TipoTecnologia {nombre: $nombre_tipo})
                CREATE (b)-[:TIENE_TECNOLOGIA]->(tt)
            """, id_biblioteca=datos_biblioteca["biblioteca"]["id"], nombre_tipo=tt["nombre"])

        # Crear y vincular nodos TipoPoblacion
        for tp in datos_biblioteca["tipos_poblacion"]:
            tx.run("""
                MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
                MERGE (tp:TipoPoblacion {nombre: $nombre_tipo})
                CREATE (b)-[:ATIENDE]->(tp)
            """, id_biblioteca=datos_biblioteca["biblioteca"]["id"], nombre_tipo=tp["nombre"])

        # Crear y vincular nodos TipoAliado
        for ta in datos_biblioteca["tipos_aliados"]:
            tx.run("""
                MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
                MERGE (ta:TipoAliado {nombre: $nombre_tipo})
                CREATE (b)-[:ALIADA_CON]->(ta)
            """, id_biblioteca=datos_biblioteca["biblioteca"]["id"], nombre_tipo=ta["nombre"])

        # Crear y vincular nodos TipoFinanciacion
        for tf in datos_biblioteca["tipos_financiacion"]:
            tx.run("""
                MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
                MERGE (tf:TipoFinanciacion {nombre: $nombre_tipo})
                CREATE (b)-[:FINANCIADA_POR]->(tf)
            """, id_biblioteca=datos_biblioteca["biblioteca"]["id"], nombre_tipo=tf["nombre"])

    with driver.session() as sesion:
        for datos_fila in datos:
            sesion.execute_write(crear_grafo, datos_fila)

    driver.close()


def main():
    import os

    csv_file_path = 'Contacto Bibliotecas - DB SIBIBO Comunitario.csv'  # Replace with your CSV file path
    neo4j_uri = os.getenv("NEO4J_URI")  # Replace with your Neo4j URI
    neo4j_user = os.getenv("NEO4J_USER")  # Replace with your Neo4j username
    neo4j_password = os.getenv("NEO4J_PASSWORD")  # Replace with your Neo4j password

    # Extract CSV file
    csv_data = extract_csv(csv_file_path)

    # Process CSV data into Neo4j objects
    neo4j_data = [crear_objetos_neo4j(row) for row in csv_data]

    # cargar data into Neo4j
    cargar_datos_en_neo4j(neo4j_uri, neo4j_user, neo4j_password, neo4j_data)

    print("Data successfully imported into Neo4j!")


if __name__ == "__main__":
    main()
