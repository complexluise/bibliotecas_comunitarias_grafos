from abc import ABC, abstractmethod
from typing import List, Dict


class DataSource(ABC):
    @abstractmethod
    def extract(self) -> List[Dict]:
        pass


class DataDestination(ABC):
    @abstractmethod
    def load(self, data: List[Dict]):
        pass


class DataTransformer(ABC):
    @abstractmethod
    def transform(self, data: List[Dict]) -> List[Dict]:
        pass
