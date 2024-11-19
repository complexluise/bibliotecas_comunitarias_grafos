# Mapeo de las Colecciones de las Bibliotecas Comunitarias de Bogotá

<img alt="Bibliotecas Comunitarias" height="500" src="docs/bibliotecas.webp" width="500"/>

Un proyecto de **mapeo de colecciones** que utiliza analítica de datos y un **grafo de conocimiento** para caracterizar las bibliotecas comunitarias de Bogotá, identificar sus fortalezas y necesidades, y priorizar la implementación de tecnologías como catálogos digitales.

---

## ✨ Descripción General

Este proyecto busca:

- Representar las **relaciones e interdependencias** entre bibliotecas y sus colecciones.
- Ofrecer análisis estratégicos para el **fortalecimiento** de las bibliotecas.
- Facilitar la **toma de decisiones** basada en datos.

---

## ⚙️ Características del Sistema

- **Grafo de Conocimiento**:
  - **Nodos**: Bibliotecas, colecciones, tecnologías, entre otros.
  - **Relaciones**: Ubicación, servicios, actividades, y más.

- **Análisis Multidimensional**:
  - Nivel de **digitalización** y **sistematización**.
  - **Caracterización** de colecciones.
  - Evaluación de la **adopción tecnológica**.

- **Exportación de Resultados**:
  - Reportes en CSV con métricas clave y evaluaciones.

---

## 🛠️ Metodología

1. **Recopilación de Datos**:
   - Bases de datos y formularios con información sobre bibliotecas y colecciones.
2. **Construcción del Grafo**:
   - Utiliza Neo4j para modelar bibliotecas y sus relaciones.
3. **Análisis**:
   - Dimensiones específicas y representaciones espaciales.
4. **Tipificación**:
   - Agrupación de bibliotecas por similitudes.
5. **Priorización Tecnológica**:
   - Identificación de bibliotecas aptas para la implementación de catálogos digitales.

---

## 📂 Estructura del Proyecto

```plaintext
├── data/                    # Datos de entrada
├── output/                  # Resultados del análisis
└── etl/                     # Código fuente
    ├── core/               # Componentes base
    ├── sources/           # Fuentes de datos
    ├── transformers/      # Transformadores
    ├── destinations/      # Destinos de datos
    └── coordinates/       # Cálculo de coordenadas
```

---

## 🚀 Requisitos

### Instalación

```bash
git clone https://github.com/complexluise/bibliotecas_comunitarias_grafos
cd bibliotecas_comunitarias_grafos
pip install -r requirements.txt
```

### Configuración

- **Credenciales de Neo4j**: Configura las variables de entorno.
- **Archivos CSV en `data/`**:
  - `libraries.csv` (Base de datos de bibliotecas comunitarias).
  - `coordinates.csv` (Respuestas formulario con coordenadas).

---

## 🛠️ Uso

### 1. Preparar los datos

Coloca los archivos CSV requeridos en la carpeta `data/`:


### 2. Ejecutar el CLI

El sistema ofrece tres comandos principales:

#### a) Procesar todo el pipeline

```bash
python -m etl.cli process-all --input-libraries "data/libraries.csv" --input-coords "data/coordinates.csv" --output "output/results.csv"
```

#### b) Procesar solo el grafo de conocimiento

```bash
python -m etl.cli knowledge-graph --input-libraries "data/libraries.csv"
```

#### c) Procesar solo la operacionalización

```bash
python -m etl.cli operationalization --output "output/results.csv"
```

### Opciones comunes

| **Opción**             | **Descripción**                                 | **Predeterminado**             |
|-------------------------|-----------------------------------------------|--------------------------------|
| `--neo4j-uri`          | URI de conexión a Neo4j                       | `bolt://localhost:7687`       |
| `--neo4j-user`         | Usuario de Neo4j                              | `neo4j`                       |
| `--neo4j-password`     | Contraseña de Neo4j                           | *(Se solicitará de forma segura)* |
| `--input-libraries`    | Archivo CSV de bibliotecas                    | Requerido para `process-all` y `knowledge-graph` |
| `--input-coords`       | Archivo CSV de coordenadas                    | Requerido para `process-all` |
| `--output`             | Archivo CSV de resultados                     | Requerido para `process-all` y `operationalization` |

#### Ejemplo completo

```bash
python -m etl.cli process-all \
  --neo4j-uri "bolt://localhost:7687" \
  --neo4j-user "neo4j" \
  --input-libraries "data/libraries.csv" \
  --input-coords "data/coordinates.csv" \
  --output "output/analysis_results.csv"
```

---

## 📊 Resultados

1. **Grafo de Conocimiento**:
   - Relaciones entre bibliotecas y sus características.
   - Visualización en Neo4j.

2. **Análisis**:
   - Archivo CSV con métricas por biblioteca y dimensión evaluada.

---

## 🖋️ Contribuciones

¡Las contribuciones son bienvenidas! 🎉

1. Realiza un **fork** del repositorio.
2. Crea una **rama** para tu funcionalidad.
3. Envía un **pull request**.

---

## 📜 Licencia

Este proyecto está bajo la [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).