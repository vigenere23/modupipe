import unittest
from typing import Iterator

from mockito import mock, verify, when

from modupipe.runnable import Pipeline, Retry
from modupipe.sink import NullSink, Sink
from modupipe.source import Source

VALUE_1 = 3.546
VALUE_2 = 234.123


class PipelineTest(unittest.TestCase):
    def test_itPassesSourceItemsToSink(self):
        sink = mock(Sink, strict=False)
        source = self._givenSourceReturning(iter([VALUE_1, VALUE_2]))
        pipeline = Pipeline[float](source=source, sink=sink)

        pipeline.run()

        verify(sink, inorder=True).receive(VALUE_1)
        verify(sink, inorder=True).receive(VALUE_2)

    def test_givingFailingSource_itRethrowsException(self):
        sink = NullSink()
        pipeline = Pipeline[float](source=self._givenFailingSource(), sink=sink)

        with self.assertRaises(Exception):
            pipeline.run()

    def test_givingFailingSource_itDoesNotSendToSink(self):
        sink = mock(Sink)
        pipeline = Pipeline[float](source=self._givenFailingSource(), sink=sink)

        try:
            pipeline.run()
        except Exception:
            pass
        finally:
            verify(sink, times=0).receive(...)

    def _givenFailingSource(self):
        source = mock(Source)
        when(source).fetch().thenRaise(Exception)

        return source

    def _givenSourceReturning(self, items: Iterator[float]):
        source = mock(Source)
        when(source).fetch().thenReturn(items)

        return source


class RetryTest(unittest.TestCase):
    def test_itCatchesNFailures(self):
        failing_source = self._givenFailingSource()
        pipeline = Retry(Pipeline(failing_source, NullSink()), nb_times=2)

        try:
            pipeline.run()
        except Exception:
            verify(failing_source, times=3).fetch()

    def _givenFailingSource(self):
        source = mock(Source)
        when(source).fetch().thenRaise(Exception)

        return source
