from abc import ABC, abstractmethod
from pandas import DataFrame


class AnalysisCoordinate(ABC):
    def __init__(
        self, driver=None, df_encuestas=None, name: str = "", description: str = ""
    ):
        self.driver = driver
        self.df_encuestas = df_encuestas
        self.category = None
        self.name = name
        self.description = description
        self.score = None

    @abstractmethod
    def get_data(self) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def calculate_score(self, bibliotecas: list[str]) -> DataFrame:
        raise NotImplementedError
