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

## Uso

1. **Preparar los datos**:
   - Coloca los archivos CSV requeridos en la carpeta `data/`:
     - `BASE DE DATOS DE BIBLIOTECAS COMUNITARIAS DE BOGOTÁ - SIBIBO 2024 - Base de datos.csv`
     - `Contacto Bibliotecas - Formulario Coordenadas.csv`

2. **Ejecutar el CLI**:

   El sistema provee varios comandos a través de su interfaz CLI:

   - Procesar todo el pipeline (grafo y operacionalización):
     ```bash
     python -m etl process-all
     ```

   - Procesar solo el grafo de conocimiento:
     ```bash
     python -m etl knowledge-graph
     ```

   - Procesar solo la operacionalización:
     ```bash
     python -m etl operationalization
     ```

   Opciones disponibles para todos los comandos:
   - `--neo4j-uri`: URI de conexión a Neo4j (default: "bolt://localhost:7687")
   - `--neo4j-user`: Usuario de Neo4j (default: "neo4j")
   - `--neo4j-password`: Contraseña de Neo4j (se solicitará de forma segura)

   Ejemplo con parámetros personalizados:
   ```bash
   python -m etl process-all --neo4j-uri "bolt://myserver:7687" --neo4j-user "admin"

3. **Resultados**:
   - **Grafo de conocimiento**: Relaciones entre bibliotecas y sus características.
   - **Análisis**: Archivo CSV con métricas por biblioteca y dimensión evaluada.


## Contribuciones

- Realiza un fork del repositorio.
- Crea una rama para la funcionalidad que deseas agregar.
- Envíanos un pull request.

## Licencia

Este proyecto está bajo Apache License Version 2.0.