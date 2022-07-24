from typing import Generic, TypeVar

from pipeline.sink import Sink
from pipeline.source import Source

Data = TypeVar("Data")


class Pipeline(Generic[Data]):
    def __init__(
        self, source: Source[Data], sink: Sink[Data]
    ) -> None:
        self.source = source
        self.sink = sink

    def run(self, max_iterations: int = None):
        for i, item in enumerate(self.source.get()):
            if max_iterations and i >= max_iterations:
                break

            self.sink.receive(item)
