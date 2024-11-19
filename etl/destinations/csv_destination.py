from etl.core.base import DataDestination
import pandas as pd


class CSVDestination(DataDestination):
    def __init__(self, output_path: str):
        self.output_path = output_path

    def load(self, data: pd.DataFrame):
        self._write(data)

    def _write(self, data: pd.DataFrame):
        data.to_csv(self.output_path, index=False)
