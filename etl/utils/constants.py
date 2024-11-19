from enum import Enum


class AnalysisCategory(Enum):
    CATALOGO_DIGITALIZACION = "Nivel de avance en la digitalización del catálogo"
    SISTEMATIZACION_SERVICIOS = (
        "Nivel de avance en la sistematización de servicios bibliotecarios"
    )
    COLECCION_CARACTERIZACION = "Caracterización de la colección"
    FACILIDAD_ADOPCION_KOHA = "Facilidad de Adopción del Catálogo en Koha"
    IMPACTO_ADOPTAR_KOHA = "Impacto de Adoptar Koha"
    DETALLE_COLECCION_KOHA = "Detalle Colección Koha"
