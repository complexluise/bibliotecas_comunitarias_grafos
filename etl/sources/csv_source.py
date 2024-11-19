from etl.core.base import DataSource
from etl.utils.utils import extract_csv


class CSVDataSource(DataSource):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract(self) -> list[dict]:
        return extract_csv(self.file_path)
