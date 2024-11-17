import csv
import os
from neo4j import GraphDatabase


def safe_float(value, default=None):
    try:
        return float(value) if value else default
    except ValueError:
        return default


def safe_int(value, default=None):
    try:
        return int(value) if value else default
    except ValueError:
        return default


def extract_csv(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        return list(csv_reader)


def crear_objetos_neo4j(row):
    biblioteca = {
        "ID": row.get("ID", ""),
        "Nombre": row.get("Nombre de la organización", ""),
        "Estado": row.get("Estado", ""),
        "Representante": row.get("Representante (Nombre y Apellido)", ""),
        "Direccion": row.get("Dirección", ""),
        "Barrio": row.get("Barrio", ""),
        "Latitud": safe_float(row.get("7_latitud")),
        "Longitud": safe_float(row.get("8_longitud"))
    }

    catalogo = {
        "SoporteCatalogo": safe_int(row.get("29_soporte_catalogo")),
        "PorcentajeCatalogado": safe_float(row.get("porcentaje_coleccion_catalogada")),
        "NivelInformacion": safe_int(row.get("nivel_informacion_catalogo"))
    }

    infraestructura = {
        "Conectividad": safe_int(row.get("32_conectividad")),
        "EquipamientoComputadores": safe_int(row.get("33_tic_computadores"))
    }

    coleccion = {
        "CantidadInventario": safe_int(row.get("24_cantidad_inventario")),
        "PorcentajeBuenEstado": safe_float(row.get("porcentaje_coleccion_buen_estado")),
        "EspecificidadTopicos": safe_int(row.get("especificidad_topicos")),
        "ActividadesMediacion": safe_int(row.get("actividades_mediacion")),
        "RarezaColeccion": safe_int(row.get("rareza_coleccion")),
        "AccesoColeccion": safe_int(row.get("acceso_coleccion")),
        "TiempoBusquedaLibro": safe_int(row.get("tiempo_busqueda_libro")),
        "NivelOrdenColeccion": safe_int(row.get("nivel_orden_coleccion")),
        "SistemasClasificacion": row.get("sistemas_clasificacion", "")
    }

    tipos_coleccion = {f"Tiene{k.capitalize()}": v for k, v in row.items() if k.startswith("26_tipo_coleccion_")}
    coleccion.update(tipos_coleccion)

    servicios = {f"Ofrece{k.capitalize()}": v for k, v in row.items() if k.startswith("30_servicios_")}
    actividades = {f"Realiza{k.capitalize()}": v for k, v in row.items() if k.startswith("31_actividades_")}
    publico = {f"Atiende{k.capitalize()}": v for k, v in row.items() if k.startswith("34_poblacion_")}

    implementacion_koha = {
        "NivelInteres": safe_int(row.get("nivel_interes_catalogo")),
        "SobrecargaAdministrativa": safe_int(row.get("sobrecarga_admin_catalogo")),
        "BeneficiosOperacionales": safe_int(row.get("beneficios_operacionales_catalogo")),
        "RiesgoResistenciaCambio": row.get("riesgo_resistencia_al_cambio", ""),
        "AccesibilidadCatalogoComunidad": row.get("accesibilidad_catalogo_comunidad", ""),
        "CapacidadTecnicaPersonal": row.get("capacidad_tecnica_personal", ""),
        "CompromisoPersonalVoluntario": row.get("compromiso_personal_voluntario", "")
    }

    return {
        "Biblioteca": biblioteca,
        "Catalogo": catalogo,
        "Infraestructura": infraestructura,
        "Coleccion": coleccion,
        "Servicios": servicios,
        "Actividades": actividades,
        "Publico": publico,
        "ImplementacionKoha": implementacion_koha
    }


def cargar_datos_en_neo4j(uri, user, password, data):
    driver = GraphDatabase.driver(uri, auth=(user, password))

    def create_nodes(tx, item):
        # Crear nodo Biblioteca
        tx.run("""
            CREATE (b:Biblioteca)
            SET b += $BibliotecaProps
        """, {"BibliotecaProps": {k: v for k, v in item["Biblioteca"].items() if v is not None}})

        # Crear nodo Catálogo y relacionarlo con Biblioteca
        tx.run("""
            MATCH (b:Biblioteca {ID: $BibliotecaID})
            CREATE (c:Catalogo)
            SET c += $CatalogoProps
            CREATE (b)-[:TIENE_CATALOGO]->(c)
        """, {"BibliotecaID": item["Biblioteca"]["ID"],
              "CatalogoProps": {k: v for k, v in item["Catalogo"].items() if v is not None}})

        # Crear nodo Infraestructura y relacionarlo con Biblioteca
        tx.run("""
            MATCH (b:Biblioteca {ID: $BibliotecaID})
            CREATE (i:Infraestructura)
            SET i += $InfraestructuraProps
            CREATE (b)-[:TIENE_INFRAESTRUCTURA]->(i)
        """, {"BibliotecaID": item["Biblioteca"]["ID"],
              "InfraestructuraProps": {k: v for k, v in item["Infraestructura"].items() if v is not None}})

        # Crear nodo Colección y relacionarlo con Biblioteca
        tx.run("""
            MATCH (b:Biblioteca {ID: $BibliotecaID})
            CREATE (c:Coleccion)
            SET c += $ColeccionProps
            CREATE (b)-[:TIENE_COLECCION]->(c)
        """, {"BibliotecaID": item["Biblioteca"]["ID"],
              "ColeccionProps": {k: v for k, v in item["Coleccion"].items() if v is not None}})

        # Crear nodos Servicios, Actividades, Público y relacionarlos con Biblioteca
        for entity_type in ["Servicios", "Actividades", "Publico"]:
            tx.run(f"""
                MATCH (b:Biblioteca {{ID: $BibliotecaID}})
                CREATE (e:{entity_type})
                SET e += $EntityProps
                CREATE (b)-[:OFRECE_{entity_type.upper()}]->(e)
            """, {"BibliotecaID": item["Biblioteca"]["ID"],
                  "EntityProps": {k: v for k, v in item[entity_type].items() if v is not None}})

        # Crear nodo ImplementacionKoha y relacionarlo con Biblioteca
        tx.run("""
            MATCH (b:Biblioteca {ID: $BibliotecaID})
            CREATE (i:ImplementacionKoha)
            SET i += $ImplementacionKohaProps
            CREATE (b)-[:PLANEA_IMPLEMENTAR]->(i)
        """, {"BibliotecaID": item["Biblioteca"]["ID"],
              "ImplementacionKohaProps": {k: v for k, v in item["ImplementacionKoha"].items() if v is not None}})

    with driver.session() as session:
        for item in data:
            session.execute_write(create_nodes, item)

    driver.close()


def main():
    csv_file_path = 'data/Contacto Bibliotecas - Mapeo Bibliotecas.csv'  # Reemplaza con la ruta de tu archivo CSV
    neo4j_uri = os.getenv("NEO4J_URI")
    neo4j_user = os.getenv("NEO4J_USER")
    neo4j_password = os.getenv("NEO4J_PASSWORD")

    # Extraer datos del CSV
    csv_data = extract_csv(csv_file_path)

    # Procesar datos del CSV en objetos para Neo4j
    neo4j_data = [crear_objetos_neo4j(row) for row in csv_data]

    # Cargar datos en Neo4j
    cargar_datos_en_neo4j(neo4j_uri, neo4j_user, neo4j_password, neo4j_data)

    print("¡Datos importados exitosamente a Neo4j!")


if __name__ == "__main__":
    main()