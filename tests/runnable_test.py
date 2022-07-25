import unittest
from typing import Iterator

from pipeline.runnable import Pipeline
from pipeline.sink import NullSink
from pipeline.source import Source

A_VALUE = 3.546


class FakeSource(Source[float]):
    def get(self) -> Iterator[float]:
        while True:
            yield A_VALUE


class PipelineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.pipeline = Pipeline[float](source=FakeSource(), sink=NullSink())
