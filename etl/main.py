from etl.utils.utils import setup_logger
from etl.utils.models import Neo4JConfig
from etl.core.pipeline import ETLPipeline
from etl.core.observer import ETLObserver
from etl.sources.csv_source import CSVDataSource
from etl.destinations.neo4j_destination import Neo4jDestination
from etl.transformers.bibliotecas_transformer import BibliotecasTransformer

registrador = setup_logger("etl_main.log")


def main():
    config = Neo4JConfig()

    bibliotecas_pipeline = ETLPipeline(
        source=CSVDataSource(
            "data/BASE DE DATOS DE BIBLIOTECAS COMUNITARIAS DE BOGOTÁ - SIBIBO 2024 - Base de datos.csv"
        ),
        transformer=BibliotecasTransformer(),
        destination=Neo4jDestination(config),
    )

    bibliotecas_pipeline.add_observer(ETLObserver())
    bibliotecas_pipeline.execute()


if __name__ == "__main__":
    main()
