from typing import Dict
from .neo4j_destination import Neo4jDestination


class BibliotecaNeo4jDestination(Neo4jDestination):

    @staticmethod
    def _process_record(tx, biblioteca_data: Dict):
        # Create main biblioteca node
        tx.run(
            """
            MERGE (b:BibliotecaComunitaria {id: $props.id})
            SET b += $props
        """,
            props=biblioteca_data["biblioteca"],
        )

        # Create relationships with standard mapping
        relationships_map = {
            "ubicacion": "UBICADA_EN",
            "localidad": "PERTENECE_A",
            "redes_sociales": "TIENE_REDES",
            "coleccion": "TIENE_COLECCION",
            "catalogo": "TIENE_CATALOGO",
            "tecnologia": "USA_TECNOLOGIA",
        }

        for field, relationship in relationships_map.items():
            if field in biblioteca_data:
                tx.run(
                    f"""
                    MATCH (b:BibliotecaComunitaria {{id: $biblioteca_id}})
                    MERGE (n:{field.title()} {{id: $props.id}})
                    SET n += $props
                    MERGE (b)-[:{relationship}]->(n)
                """,
                    biblioteca_id=biblioteca_data["biblioteca"]["id"],
                    props=biblioteca_data[field],
                )

        # Handle list relationships
        list_relationships = {
            "tipos_coleccion": "CONTIENE_TIPO",
            "tipos_servicio": "OFRECE_SERVICIO",
            "tipos_actividad": "REALIZA_ACTIVIDAD",
            "tipos_tecnologia": "TIENE_TECNOLOGIA",
            "tipos_poblacion": "ATIENDE",
            "tipos_aliados": "ALIADA_CON",
            "tipos_financiacion": "FINANCIADA_POR",
        }

        for field, relationship in list_relationships.items():
            if field in biblioteca_data:
                for item in biblioteca_data[field]:
                    tx.run(
                        f"""
                        MATCH (b:BibliotecaComunitaria {{id: $biblioteca_id}})
                        MERGE (t:{field.title()[:-1]} {{nombre: $nombre}})
                        MERGE (b)-[:{relationship}]->(t)
                    """,
                        biblioteca_id=biblioteca_data["biblioteca"]["id"],
                        nombre=item["nombre"],
                    )
