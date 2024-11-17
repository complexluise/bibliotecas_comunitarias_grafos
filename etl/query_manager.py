class Neo4JQueryManager:

    @staticmethod
    def create_biblioteca_comunitaria() -> str:
        return """
        CREATE (b:BibliotecaComunitaria)
        SET b += $props_biblioteca
        """

    @staticmethod
    def create_and_link_ubicacion() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        CREATE (u:Ubicacion)
        SET u += $props_ubicacion
        CREATE (b)-[:UBICADA_EN]->(u)
        """

    @staticmethod
    def create_and_link_localidad() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        MERGE (l:Localidad {nombre: $nombre_localidad})
        CREATE (b)-[:PERTENECE_A]->(l)
        """

    @staticmethod
    def create_and_link_redes_sociales() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        CREATE (rs:RedesSociales)
        SET rs += $props_redes_sociales
        CREATE (b)-[:TIENE_REDES_SOCIALES]->(rs)
        """

    @staticmethod
    def create_and_link_coleccion() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        CREATE (c:Coleccion)
        SET c += $props_coleccion
        CREATE (b)-[:TIENE_COLECCION]->(c)
        """

    @staticmethod
    def create_and_link_tipo_coleccion() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        MERGE (tc:TipoColeccion {nombre: $nombre_tipo})
        CREATE (b)-[:TIENE_TIPO_COLECCION]->(tc)
        """

    @staticmethod
    def create_and_link_catalogo() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        CREATE (cat:Catalogo)
        SET cat += $props_catalogo
        CREATE (b)-[:TIENE_CATALOGO]->(cat)
        """

    @staticmethod
    def create_and_link_soporte_catalogo() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        CREATE (sc:SoporteCatalogo)
        SET sc += $props_soporte_catalogo
        CREATE (b)-[:USA_SOPORTE_CATALOGO]->(sc)
        """

    @staticmethod
    def create_and_link_tipo_servicio() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        MERGE (ts:TipoServicio {nombre: $nombre_tipo})
        CREATE (b)-[:OFRECE]->(ts)
        """

    @staticmethod
    def create_and_link_tipo_actividad() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        MERGE (ta:TipoActividad {nombre: $nombre_tipo})
        CREATE (b)-[:REALIZA]->(ta)
        """

    @staticmethod
    def create_and_link_tecnologia() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        CREATE (t:Tecnologia)
        SET t += $props_tecnologia
        CREATE (b)-[:USA_TECNOLOGIA]->(t)
        """

    @staticmethod
    def create_and_link_tipo_tecnologia() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        MERGE (tt:TipoTecnologia {nombre: $nombre_tipo})
        CREATE (b)-[:TIENE_TECNOLOGIA]->(tt)
        """

    @staticmethod
    def create_and_link_tipo_poblacion() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        MERGE (tp:TipoPoblacion {nombre: $nombre_tipo})
        CREATE (b)-[:ATIENDE]->(tp)
        """

    @staticmethod
    def create_and_link_tipo_aliado() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        MERGE (ta:TipoAliado {nombre: $nombre_tipo})
        CREATE (b)-[:ALIADA_CON]->(ta)
        """

    @staticmethod
    def create_and_link_tipo_financiacion() -> str:
        return """
        MATCH (b:BibliotecaComunitaria {id: $id_biblioteca})
        MERGE (tf:TipoFinanciacion {nombre: $nombre_tipo})
        CREATE (b)-[:FINANCIADA_POR]->(tf)
        """

    @staticmethod
    def infraestructura_tecnologica():
        return """MATCH (b:BibliotecaComunitaria)
        OPTIONAL MATCH (b)-[r1:TIENE_TECNOLOGIA]->(t1:TipoTecnologia)
        OPTIONAL MATCH (b)-[r2:USA_TECNOLOGIA]->(t2:Tecnologia) 
        RETURN 
            b.id AS BibliotecaID, 
            b.nombre AS BibliotecaNombre, 
            CASE WHEN t1.nombre = "computadores" THEN true ELSE false END AS tieneComputador,
            t2.conectividad AS tieneConectividad
        """

    @staticmethod
    def diversidad_colecciones():
        return """
        MATCH (b:BibliotecaComunitaria)-[:TIENE_COLECCION]->(c:Coleccion)
        RETURN b.id AS BibliotecaID, collect(DISTINCT c.tipo) AS tipos_coleccion
        """

    @staticmethod
    def cantidad_material_bibliografico():
        return """
        MATCH (b:BibliotecaComunitaria)
        RETURN b.id AS BibliotecaID, b.cantidad_inventario AS cantidad_inventario
        """

    @staticmethod
    def diversidad_servicios():
        return """
        MATCH (b:BibliotecaComunitaria)-[:OFRECE_SERVICIO]->(s:Servicio)
        RETURN b.id AS BibliotecaID, collect(DISTINCT s.tipo) AS servicios
        """

    @staticmethod
    def tipos_coleccion():
        return """
        MATCH (b:BibliotecaComunitaria)-[:TIENE_COLECCION]->(c:Coleccion)
        RETURN b.id AS BibliotecaID, collect(DISTINCT c.tipo) AS tipos_coleccion
        """