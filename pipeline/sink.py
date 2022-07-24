from abc import ABC, abstractmethod
from typing import Any, Generic, List, TypeVar

Input = TypeVar("Input")


class Sink(ABC, Generic[Input]):
    @abstractmethod
    def push(self, input: Input) -> None:
        pass


class SinkList(Sink[Input], Generic[Input]):
    def __init__(self, sinks: List[Sink[Input]]) -> None:
        self.sinks = sinks

    def push(self, input: Input) -> None:
        for sink in self.sinks:
            sink.push(input)


class Print(Sink[Any]):
    def push(self, input: Input) -> None:
        print(input)
