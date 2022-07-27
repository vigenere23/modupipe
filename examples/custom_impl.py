from typing import Any, Iterator

from modupipe.mapper import Mapper
from modupipe.runnable import Pipeline
from modupipe.sink import Sink
from modupipe.source import Source


class IncrementalSource(Source[int]):
    def fetch(self) -> Iterator[int]:
        i = 0

        while True:
            yield i
            i += 1


class Divider(Mapper[int, float]):
    def __init__(self, divider: int) -> None:
        self.divider = divider

    def map(self, items: Iterator[int]) -> Iterator[float]:
        for item in items:
            yield item / self.divider


class CustomPrinter(Sink[Any]):
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix

    def receive(self, item: Any) -> None:
        print(f"{self.prefix}{item}")


source = IncrementalSource() + Divider(2)
sink = CustomPrinter("Value is : ")

pipeline = Pipeline(source, sink)

pipeline.run()
