from abc import ABC, abstractmethod
from random import random
from typing import Generic, Iterator, TypeVar

from pipeline.mapper import Mapper

Data = TypeVar("Data")
MappedData = TypeVar("MappedData")


class Source(ABC, Generic[Data]):
    @abstractmethod
    def get(self) -> Iterator[Data]:
        pass


class MappedSource(Source[MappedData], Generic[Data, MappedData]):
    def __init__(self, source: Source[Data], mapper: Mapper[Data, MappedData]) -> None:
        self.source = source
        self.mapper = mapper

    def get(self) -> Iterator[MappedData]:
        return self.mapper.map(self.source.get())


class RandomSource(Source[float]):
    def __init__(self, nb_iterations: int) -> None:
        self.nb_iterations = nb_iterations

    def get(self) -> Iterator[float]:
        for _ in range(self.nb_iterations):
            yield random()
