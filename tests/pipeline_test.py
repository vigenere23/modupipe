from typing import Iterator
from pipeline.sink import NullSink
from pipeline.source import Source
from pipeline.base import Pipeline
import unittest


A_VALUE = 3.546


class FakeSource(Source[float]):
    def get(self) -> Iterator[float]:
        while True:
            yield A_VALUE


class PipelineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.steam = Pipeline(
            source=FakeSource(),
            sink=NullSink()
        )
