# Mapeo de las Colecciones de las Bibliotecas Comunitarias de Bogotá

## Descripción General

Este proyecto realiza el **mapeo de colecciones** en las bibliotecas comunitarias de Bogotá, con un enfoque basado en **analítica de datos** y **representación relacional**. A través de un grafo de conocimiento, el sistema caracteriza las colecciones, las interrelaciones entre las bibliotecas y sus características clave, lo que permite generar análisis estratégicos para el fortalecimiento de estas instituciones.

## Características del Sistema

- **Grafo de Conocimiento**:
  - Nodos: Representan entidades como bibliotecas, colecciones, y tecnologías.
  - Relaciones: Capturan interacciones y dependencias entre nodos, como ubicación, servicios, y actividades.

- **Análisis Multidimensional**:
  - Nivel de digitalización y sistematización.
  - Caracterización de colecciones.
  - Evaluación de la facilidad de adopción de tecnología.

- **Exportación de Resultados**:
  - Reportes en formato CSV con métricas clave y evaluaciones.

## Metodología

1. **Recopilación de Datos**: Se utilizan bases de datos y formularios que contienen información sobre bibliotecas y colecciones.
2. **Construcción del Grafo**: Neo4j es utilizado para modelar las bibliotecas y sus relaciones clave.
3. **Análisis**:
   - Evaluación de dimensiones específicas de las colecciones.
   - Representación espacial de bibliotecas según sus características.
4. **Tipificación**: Identificación de categorías que agrupan a las bibliotecas según similitudes y diferencias.
5. **Priorización Tecnológica**: Identificación de bibliotecas más aptas para la implementación de un catálogo digital.

## Estructura del Proyecto

```
├── data/                    # Datos de entrada
├── output/                  # Resultados del análisis
└── etl/                     # Código fuente
    ├── core/               # Componentes base
    ├── sources/           # Fuentes de datos
    ├── transformers/      # Transformadores
    ├── destinations/      # Destinos de datos
    └── coordinates/       # Cálculo de coordenadas
```

## Requisitos

### Instalación

```bash
git clone https://github.com/complexluise/bibliotecas_comunitarias_grafos
cd bibliotecas_comunitarias_grafos
pip install -r requirements.txt
```

### Configuración

- Credenciales de Neo4j en las variables de entorno.
- Archivos CSV colocados en `data/`:
  - Base de datos de bibliotecas comunitarias.
  - Formulario con coordenadas.

## Uso

1. **Preparar los datos**:
   - Asegúrate de que los archivos CSV requeridos están en la carpeta `data/`.

2. **Ejecutar el pipeline**:

   ```bash
   python -m etl process-all
   ```

3. **Resultados**:
   - **Grafo de conocimiento**: Relaciones entre bibliotecas y sus características.
   - **Análisis**: Archivo CSV con métricas por biblioteca y dimensión evaluada.


## Contribuciones

- Realiza un fork del repositorio.
- Crea una rama para la funcionalidad que deseas agregar.
- Envíanos un pull request.

## Licencia

Este proyecto está bajo Apache License Version 2.0.