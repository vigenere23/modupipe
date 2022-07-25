import unittest
from typing import Iterator
from unittest.mock import MagicMock, call

from modupipe.runnable import Pipeline
from modupipe.sink import NullSink
from modupipe.source import Source

VALUE = 3.546


class FakeSource(Source[float]):
    def fetch(self) -> Iterator[float]:
        yield VALUE
        yield VALUE


class FailingSource(Source[None]):
    def fetch(self) -> Iterator[None]:
        raise Exception()


class PipelineTest(unittest.TestCase):
    def test_itPassesSourceItemsToSink(self):
        sink = NullSink()
        sink.receive = MagicMock()  # type: ignore
        pipeline = Pipeline[float](source=FakeSource(), sink=sink)

        pipeline.run()

        sink.receive.assert_has_calls([call(VALUE), call(VALUE)])

    def test_givingFailingSource_itRethrowsException(self):
        sink = NullSink()
        pipeline = Pipeline[float](source=FailingSource(), sink=sink)

        with self.assertRaises(Exception):
            pipeline.run()

    def test_givingFailingSource_itDoesNotSendToSink(self):
        sink = NullSink()
        sink.receive = MagicMock()  # type: ignore
        pipeline = Pipeline[float](source=FailingSource(), sink=sink)

        try:
            pipeline.run()
        except Exception:
            pass
        finally:
            sink.receive.assert_not_called()
