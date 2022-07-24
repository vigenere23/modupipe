from typing import Generic, TypeVar

from pipeline.mapper import Mapper
from pipeline.sink import Sink
from pipeline.source import Source

Input = TypeVar("Input")
Output = TypeVar("Output")


class Stream(Generic[Input, Output]):
    def __init__(
        self, source: Source[Input], mapper: Mapper[Input, Output], sink: Sink[Output]
    ) -> None:
        self.source = source
        self.mapper = mapper
        self.sink = sink

    def start(self, max_iterations: int = None):
        iterator = self.mapper.map(self.source.get())

        for i, item in enumerate(iterator):
            if max_iterations and i >= max_iterations:
                break

            self.sink.push(item)
