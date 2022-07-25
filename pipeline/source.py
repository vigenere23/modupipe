from abc import ABC, abstractmethod
from random import random
from typing import Generic, Iterator, TypeVar

from pipeline.exceptions import MaxIterationsReached
from pipeline.mapper import Mapper
from pipeline.queue import Queue, QueueGetStrategy

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


class QueueSource(Source[Data]):
    def __init__(self, queue: Queue[Data], strategy: QueueGetStrategy[Data]) -> None:
        self.queue = queue
        self.strategy = strategy

    def get(self) -> Iterator[Data]:
        while True:
            yield self.strategy.get(self.queue)


class RandomSource(Source[float]):
    def get(self) -> Iterator[float]:
        while True:
            yield random()


class MaxIterations(Source[Data]):
    def __init__(self, source: Source[Data], nb_iterations: int) -> None:
        self.source = source
        self.nb_iterations = nb_iterations

    def get(self) -> Iterator[Data]:
        for i, item in enumerate(self.source.get()):
            if i >= self.nb_iterations:
                raise MaxIterationsReached()

            yield item
