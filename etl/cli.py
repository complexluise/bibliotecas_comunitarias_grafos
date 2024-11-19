import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from etl.core import ETLPipeline, ETLObserver
from etl.destinations import BibliotecaNeo4jDestination, CSVDestination
from etl.sources import OperationalizationSource, CSVDataSource
from etl.transformers import BibliotecasTransformer, OperationalizationTransformer
from etl.utils import setup_logger, Neo4JConfig

console = Console()
logger = setup_logger("etl.log", "etl.cli")


def process_with_progress(func, *args, **kwargs):
    """Ejecuta una función con un indicador de progreso.

    Args:
        func (callable): La función a ejecutar con seguimiento de progreso.
        *args: Argumentos posicionales para pasar a la función
        **kwargs: Argumentos con nombre para pasar a la función
    """
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        progress.add_task(description=f"Procesando  {func.__name__}...", total=None)
        func(*args, **kwargs)


def process_knowledge_graph(config: Neo4JConfig, library_data_path: str):
    """Procesa y carga datos del grafo de conocimiento en Neo4j.

    Args:
        config (Neo4JConfig): Objeto de configuración para la conexión Neo4j.
        library_data_path (str): Ruta al archivo CSV que contiene datos de bibliotecas.
    """
    pipeline = ETLPipeline(
        source=CSVDataSource(library_data_path),
        transformer=BibliotecasTransformer(),
        destination=BibliotecaNeo4jDestination(config),
    )
    pipeline.add_observer(ETLObserver())
    pipeline.execute()


def process_operationalization(
    neo4j_config: Neo4JConfig, survey_data_path: str, output_path: str
):
    """Procesa datos de operacionalización y exporta a CSV.

    Args:
        neo4j_config (Neo4JConfig): Objeto de configuración para la conexión Neo4j.
        survey_data_path (str): Ruta al archivo CSV de datos de la encuesta.
        output_path (str): Ruta para los resultados del análisis.
    """
    pipeline = ETLPipeline(
        source=OperationalizationSource(neo4j_config, survey_data_path),
        transformer=OperationalizationTransformer(),
        destination=CSVDestination(output_path),
    )
    pipeline.add_observer(ETLObserver())
    pipeline.execute()


@click.group()
def cli():
    """Herramienta CLI para procesar datos de Bibliotecas Comunitarias.

    Esta herramienta procesa y analiza bibliotecas comunitarias en Bogotá a través de:
    - Construcción de Grafo de Conocimiento: Modela bibliotecas y sus relaciones clave en Neo4j
    - Análisis Multidimensional: Evalúa niveles de digitalización, colecciones y adopción tecnológica
    - Exportación de Datos: Genera CSV con datos procesados

    Uso:
        # Procesar grafo de conocimiento y operacionalización
        python -m etl process-all --library-data data/libraries.csv --survey-data data/survey.csv --analysis-output output/results.csv

        # Procesar solo grafo de conocimiento
        python -m etl knowledge-graph --library-data data/libraries.csv

        # Procesar solo operacionalización
        python -m etl.cli operationalization --survey-data data/survey.csv --output output/results.csv
    """
    console.print(
        Panel.fit(
            "🏛️ Herramienta de Procesamiento de Grafos para Bibliotecas Comunitarias",
            border_style="blue",
        )
    )


@cli.command()
@click.option(
    "--survey-data", required=True, help="Path to libraries survey data CSV file"
)
@click.option(
    "--output", required=True, help="Path to output analysis results CSV file"
)
def operationalization(survey_data, output):
    """Procesa los datos de operacionalización de las respuestas de la encuesta.

    Args:
        survey_data (str): Ruta al archivo CSV de datos de la encuesta
        output (str): Ruta para guardar los resultados del análisis en CSV
    """
    neo4j_config = Neo4JConfig()
    process_with_progress(process_operationalization, neo4j_config, survey_data, output)
    console.print("[bold green]✨ ¡Análisis de operacionalización completado!")


@cli.command()
@click.option("--library-data", required=True, help="Path to libraries CSV file")
def knowledge_graph(library_data):
    """Procesa solo los datos del grafo de conocimiento.

    Args:
        library_data (str): Ruta al archivo CSV de bibliotecas
    """
    config = Neo4JConfig()
    process_with_progress(process_knowledge_graph, config, library_data)
    console.print("[bold green]✨ ¡Procesamiento del grafo de conocimiento completado!")


@cli.command()
@click.option(
    "--library-data", required=True, help="Path to libraries master data CSV file"
)
@click.option(
    "--survey-data", required=True, help="Path to libraries survey data CSV file"
)
@click.option(
    "--analysis-output", required=True, help="Path to analysis results CSV file"
)
def process_all(library_data, survey_data, analysis_output):
    """Procesa tanto el grafo de conocimiento como el análisis de operacionalización.

    Args:
        library_data (str): Ruta al archivo CSV maestro de bibliotecas
        survey_data (str): Ruta al archivo CSV de datos de la encuesta
        analysis_output (str): Ruta para los resultados del análisis
    """
    neo4j_config = Neo4JConfig()

    with console.status("[bold green]Procesando datos...") as status:
        console.print("[bold blue]Iniciando construcción del grafo de conocimiento...")
        process_with_progress(process_knowledge_graph, neo4j_config, library_data)

        console.print("[bold blue]Iniciando análisis de operacionalización...")
        process_with_progress(
            process_operationalization, neo4j_config, survey_data, analysis_output
        )

    console.print("[bold green]✨ ¡Todo el procesamiento se completó exitosamente!")


if __name__ == "__main__":
    cli()
