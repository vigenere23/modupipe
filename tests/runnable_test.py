import unittest
from typing import Iterator

from mockito import mock, verify, when

from modupipe.runnable import Pipeline, Retry
from modupipe.sink import NullSink, Sink
from modupipe.source import Source

VALUE_1 = 3.546
VALUE_2 = 234.123


class FakeSource(Source[float]):
    def fetch(self) -> Iterator[float]:
        yield VALUE_1
        yield VALUE_2


class FailingSource(Source[None]):
    def fetch(self) -> Iterator[None]:
        raise Exception()


class PipelineTest(unittest.TestCase):
    def test_itPassesSourceItemsToSink(self):
        sink = mock(Sink)
        when(sink).receive(...)
        pipeline = Pipeline[float](source=FakeSource(), sink=sink)

        pipeline.run()

        verify(sink, inorder=True).receive(VALUE_1)
        verify(sink, inorder=True).receive(VALUE_2)

    def test_givingFailingSource_itRethrowsException(self):
        sink = NullSink()
        pipeline = Pipeline[float](source=FailingSource(), sink=sink)

        with self.assertRaises(Exception):
            pipeline.run()

    def test_givingFailingSource_itDoesNotSendToSink(self):
        sink = mock(Sink)
        pipeline = Pipeline[float](source=FailingSource(), sink=sink)

        try:
            pipeline.run()
        except Exception:
            pass
        finally:
            verify(sink, times=0).receive(...)


class RetryTest(unittest.TestCase):
    def test_itCatchesNFailures(self):
        source = mock(Source)
        when(source).fetch().thenRaise(Exception)
        pipeline = Retry(Pipeline(source, NullSink()), nb_times=2)

        try:
            pipeline.run()
        except Exception:
            verify(source, times=3).fetch()
