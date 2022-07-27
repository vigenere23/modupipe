from abc import ABC, abstractmethod
from typing import Any, Generic, List, TypeVar

from modupipe.queue import Queue, QueuePutStrategy

Input = TypeVar("Input")


class Sink(ABC, Generic[Input]):
    @abstractmethod
    def receive(self, item: Input) -> None:
        pass


class SinkList(Sink[Input]):
    def __init__(self, sinks: List[Sink[Input]]) -> None:
        self.sinks = sinks

    def receive(self, item: Input) -> None:
        for sink in self.sinks:
            sink.receive(item)


class NullSink(Sink[Any]):
    def receive(self, _: Any) -> None:
        pass


class Printer(Sink[Any]):
    def receive(self, item: Any) -> None:
        print(item)


class QueueSink(Sink[Input]):
    def __init__(self, queue: Queue[Input], strategy: QueuePutStrategy) -> None:
        self.queue = queue
        self.strategy = strategy

    def receive(self, item: Input) -> None:
        self.strategy.put(self.queue, item)
