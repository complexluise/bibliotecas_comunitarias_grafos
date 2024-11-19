import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from etl.destinations.csv_destination import CSVDestination
from etl.sources.operationalization_source import OperationalizationSource
from etl.transformers.operationalization_transformer import OperationalizationTransformer
from etl.utils.utils import setup_logger
from etl.utils.models import Neo4JConfig
from etl.core.pipeline import ETLPipeline
from etl.core.observer import ETLObserver
from etl.sources.csv_source import CSVDataSource
from etl.destinations.neo4j_destination import Neo4jDestination
from etl.transformers.bibliotecas_transformer import BibliotecasTransformer

console = Console()
registrador = setup_logger("etl_main.log")


def process_with_progress(func):
    """Executes a function with a progress indicator.

    Args:
        func (callable): The function to execute with progress tracking.
    """
    with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
    ) as progress:
        progress.add_task(description=f"Processing {func.__name__}...", total=None)
        func()


def process_knowledge_graph(config: Neo4JConfig):
    """Processes and loads knowledge graph data into Neo4j.

    Args:
        config (Neo4JConfig): Configuration object for Neo4j connection.
    """
    pipeline = ETLPipeline(
        source=CSVDataSource(
            "data/BASE DE DATOS DE BIBLIOTECAS COMUNITARIAS DE BOGOTÁ - SIBIBO 2024 - Base de datos.csv"
        ),
        transformer=BibliotecasTransformer(),
        destination=Neo4jDestination(config),
    )
    pipeline.add_observer(ETLObserver())
    pipeline.execute()


def process_operationalization(config: Neo4JConfig):
    """Processes operationalization data and exports to CSV.

    Args:
        config (Neo4JConfig): Configuration object for Neo4j connection.
    """
    pipeline = ETLPipeline(
        source=OperationalizationSource(
            config, "data/Contacto Bibliotecas - Formulario Coordenadas.csv"
        ),
        transformer=OperationalizationTransformer(),
        destination=CSVDestination("output/analysis_results.csv"),
    )
    pipeline.add_observer(ETLObserver())
    pipeline.execute()


@click.group()
def cli():
    """CLI tool for processing Bibliotecas Comunitarias data.

    This tool processes and analyzes community libraries in Bogotá through:
    - Knowledge Graph Construction: Models libraries and their key relationships in Neo4j
    - Multidimensional Analysis: Evaluates digitization levels, collections, and technology adoption
    - Data Export: Generates CSV with processed data


    Usage:
        # Process both knowledge graph and operationalization
        python -m etl.cli process-all --input-libraries data/libraries.csv --input-coords data/coordinates.csv --output output/results.csv

        # Process only knowledge graph
        python -m etl.cli knowledge-graph --input-libraries data/libraries.csv

        # Process only operationalization
        python -m etl.cli operationalization --input-coords data/coordinates.csv --output output/results.csv
    """
    console.print(
        Panel.fit(
            "🏛️ Bibliotecas Comunitarias Graph Processing Tool",
            border_style="blue",
        )
    )

@cli.command()
@click.option('--input-libraries', required=True, help="Path to libraries CSV file")
@click.option('--input-coords', required=True, help="Path to coordinates CSV file")
@click.option('--output', required=True, help="Path to output CSV file")
def process_all(input_libraries, input_coords, output):
    """Process both knowledge graph and operationalization data.

    Args:
        input_libraries (str): Path to libraries CSV file
        input_coords (str): Path to coordinates CSV file
        output (str): Path to output CSV file
    """
    config = Neo4JConfig()

    with console.status("[bold green]Processing data...") as status:
        console.print("[bold blue]Starting knowledge graph processing...")
        process_with_progress(lambda: process_knowledge_graph(config, input_libraries))

        console.print("[bold blue]Starting operationalization processing...")
        process_with_progress(lambda: process_operationalization(config, input_coords, output))

    console.print("[bold green]✨ All processing completed successfully!")

@cli.command()
@click.option('--input-libraries', required=True, help="Path to libraries CSV file")
def knowledge_graph(input_libraries):
    """Process only the knowledge graph data.

    Args:
        input_libraries (str): Path to libraries CSV file
    """
    config = Neo4JConfig()
    process_with_progress(lambda: process_knowledge_graph(config, input_libraries))
    console.print("[bold green]✨ Knowledge graph processing completed!")

@cli.command()
@click.option('--output', required=True, help="Path to output CSV file")
def operationalization(output):
    """Process only the operationalization data.

    Args:
        output (str): Path to output CSV file
    """
    config = Neo4JConfig()
    process_with_progress(lambda: process_operationalization(config, output))
    console.print("[bold green]✨ Operationalization processing completed!")


if __name__ == "__main__":
    cli()
