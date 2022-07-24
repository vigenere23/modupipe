from abc import ABC, abstractmethod
from random import random
from typing import Generic, Iterator, TypeVar

Data = TypeVar("Data")


class Source(ABC, Generic[Data]):
    @abstractmethod
    def get(self) -> Iterator[Data]:
        pass


class RandomSource(Source[float]):
    def __init__(self, nb_iterations: int) -> None:
        self.nb_iterations = nb_iterations

    def get(self) -> Iterator[float]:
        for _ in range(self.nb_iterations):
            yield random()
