import unittest
from typing import Iterator
from unittest.mock import MagicMock, call

from pipeline.runnable import Pipeline
from pipeline.sink import NullSink
from pipeline.source import Source

A_VALUE = 3.546


class FakeSource(Source[float]):
    def get(self) -> Iterator[float]:
        yield A_VALUE
        yield A_VALUE


class PipelineTest(unittest.TestCase):
    def test_itPassesSourceItemsToSink(self):
        sink = NullSink()
        sink.receive = MagicMock()  # type: ignore
        pipeline = Pipeline[float](source=FakeSource(), sink=sink)

        pipeline.run()

        sink.receive.assert_has_calls([call(A_VALUE), call(A_VALUE)])
