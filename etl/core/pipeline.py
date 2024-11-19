from etl.core.base import DataSource, DataTransformer, DataDestination


class ETLPipeline:
    def __init__(
        self,
        source: DataSource,
        transformer: DataTransformer,
        destination: DataDestination,
    ):
        self.source = source
        self.transformer = transformer
        self.destination = destination
        self.observers = []

    def add_observer(self, observer):
        self.observers.append(observer)

    def notify_observers(self, message: str):
        for observer in self.observers:
            observer.update(message)

    def execute(self):
        try:
            raw_data = self.source.extract()
            self.notify_observers(f"Extracted {len(raw_data)} records")

            transformed_data = self.transformer.transform(raw_data)
            self.notify_observers(f"Transformed {len(transformed_data)} records")

            self.destination.load(transformed_data)
            self.notify_observers("ETL process completed successfully")
        except Exception as e:
            self.notify_observers(f"ETL process failed: {str(e)}")
            raise
