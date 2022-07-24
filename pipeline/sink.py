from abc import ABC, abstractmethod
from typing import Any, Generic, List, TypeVar

Input = TypeVar("Input")


class Sink(ABC, Generic[Input]):
    @abstractmethod
    def receive(self, input: Input) -> None:
        pass


class SinkList(Sink[Input], Generic[Input]):
    def __init__(self, sinks: List[Sink[Input]]) -> None:
        self.sinks = sinks

    def receive(self, input: Input) -> None:
        for sink in self.sinks:
            sink.receive(input)


class NullSink(Sink[float]):
    def receive(self, _: float) -> None:
        pass


class Print(Sink[Any]):
    def receive(self, input: Input) -> None:
        print(input)
