import unittest
from typing import Any, Iterator

from mockito import mock, verify, when

from modupipe.extractor import Extractor
from modupipe.loader import Loader
from modupipe.mapper import PushTo
from modupipe.runnable import FullPipeline, NamedRunnable, Retry, Runnable, StepPipeline

VALUE_1 = 3.546
VALUE_2 = 234.123


class FakeExtractor(Extractor[float]):
    def __init__(self, items: Iterator[float]) -> None:
        self.items = items

    def extract(self) -> Iterator[float]:
        return self.items


class FailingExtractor(Extractor[Any]):
    def extract(self) -> Iterator[Any]:
        raise Exception
        yield


class FullPipelineTest(unittest.TestCase):
    def test_whenRunning_itExtractsAllItems(self):
        extractor = FakeExtractor(iter([VALUE_1, VALUE_2]))
        loader = self._givenLoader()
        pipeline = FullPipeline(extractor + PushTo(loader))

        pipeline.run()

        verify(loader, inorder=True).load(VALUE_1)
        verify(loader, inorder=True).load(VALUE_2)

    def test_givingFailingExtractor_itRethrowsException(self):
        pipeline = FullPipeline(FailingExtractor())

        with self.assertRaises(Exception):
            pipeline.run()

    def test_givingFailingExtractor_itDoesNotSendToNextModules(self):
        loader = self._givenLoader()
        pipeline = FullPipeline(FailingExtractor() + PushTo(loader))

        try:
            pipeline.run()
        except Exception:
            pass
        finally:
            verify(loader, times=0).load(...)

    def _givenLoader(self):
        return mock(Loader, strict=False)


class StepPipelineTest(unittest.TestCase):
    def test_whenRunning_itExtractsOnlyOneItem(self):
        extractor = FakeExtractor(iter([VALUE_1, VALUE_2]))
        loader = self._givenLoader()
        pipeline = StepPipeline(extractor + PushTo(loader))

        pipeline.run()

        verify(loader).load(VALUE_1)

    def test_givingFailingExtractor_itRethrowsException(self):
        pipeline = StepPipeline(FailingExtractor())

        with self.assertRaises(Exception):
            pipeline.run()

    def test_givingFailingExtractor_itDoesNotSendToNextModules(self):
        loader = self._givenLoader()
        pipeline = StepPipeline(FailingExtractor() + PushTo(loader))

        try:
            pipeline.run()
        except Exception:
            pass
        finally:
            verify(loader, times=0).load(...)

    def _givenLoader(self):
        loader = mock(Loader)
        when(loader).load(...).thenReturn(None)

        return loader


class RetryTest(unittest.TestCase):
    def test_itCatchesNFailures(self):
        failing_extractor = self._givenFailingSource()
        pipeline = Retry(FullPipeline(failing_extractor), nb_times=2)

        try:
            pipeline.run()
        except Exception:
            verify(failing_extractor, times=3).extract()

    def _givenFailingSource(self):
        extractor = mock(Extractor)
        when(extractor).extract().thenRaise(Exception)

        return extractor


class NamedRunnableTest(unittest.TestCase):
    def test_itDelegatesToSubrunnable(self):
        runnable = self._givenRunnable()
        named_runnable = NamedRunnable("test", runnable=runnable)

        named_runnable.run()

        verify(runnable).run()

    def _givenRunnable(self):
        return mock(Runnable, strict=False)
