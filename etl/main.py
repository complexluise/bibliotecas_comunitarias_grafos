from etl.utils.utils import setup_logger
from etl.utils.models import Neo4JConfig
from etl.core.pipeline import ETLPipeline
from etl.core.observer import ETLObserver
from etl.sources.csv_source import CSVDataSource
from etl.destinations.neo4j_destination import Neo4jDestination
from etl.transformers.bibliotecas_transformer import BibliotecasTransformer

registrador = setup_logger("etl_main.log")

def process_knowledge_graph(config: Neo4JConfig):
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
    pipeline = ETLPipeline(
        source=OperationalizationSource(config, "data/Contacto Bibliotecas - Formulario Coordenadas.csv"),
        transformer=OperationalizationTransformer(),
        destination=CSVDestination("output/analysis_results.csv")
    )
    pipeline.add_observer(ETLObserver())
    pipeline.execute()

def main():
    config = Neo4JConfig()
    process_knowledge_graph(config)
    process_operationalization(config)

if __name__ == "__main__":
    main()
