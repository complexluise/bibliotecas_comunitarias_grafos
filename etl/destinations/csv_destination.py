from etl.core.base import DataDestination
import pandas as pd
from pathlib import Path


class CSVDestination(DataDestination):
    def __init__(self, output_path: str):
        self.output_path = Path(output_path)

    def load(self, data: pd.DataFrame):
        self._write(data)

    def _write(self, data: pd.DataFrame):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(self.output_path, index=False)
