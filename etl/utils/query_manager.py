class Neo4JQueryManager:

    @staticmethod
    def infraestructura_tecnologica():
        return """MATCH (b:BibliotecaComunitaria)
        OPTIONAL MATCH (b)-[r1:TIENE_TECNOLOGIA]->(t1:Tipos_Tecnologia)
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
        MATCH (b:BibliotecaComunitaria)-[:CONTIENE_TIPO]->(c:Tipos_Coleccion)
        RETURN b.id AS BibliotecaID, collect(DISTINCT c.nombre) AS tipos_coleccion
        """

    @staticmethod
    def cantidad_inventario():
        return """
        MATCH (b:BibliotecaComunitaria)-[r:TIENE_COLECCION]->(c:Coleccion)
        RETURN b.id AS BibliotecaID, c.cantidad_inventario AS cantidad_inventario
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
