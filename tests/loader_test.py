import multiprocessing
import unittest

from mockito import mock, verify, when

from modupipe.base import Condition
from modupipe.loader import (
    Buffer,
    Loader,
    LoaderList,
    LoaderListUntyped,
    OnCondition,
    PutToQueue,
    ToString,
)
from modupipe.queue import PutBlocking, Queue

VALUE_1 = 439.234
VALUE_2 = 12.682


class OnConditionTest(unittest.TestCase):
    def test_itCanFilterIn(self):
        value = VALUE_1
        true_condition = self._givenConditionReturning(True)
        next_loader = self._givenLoader()
        loader = OnCondition(true_condition, next_loader)

        loader.load(value)

        verify(next_loader).load(value)

    def test_itCanFilterOut(self):
        value = VALUE_1
        false_condition = self._givenConditionReturning(False)
        next_loader = self._givenLoader()
        loader = OnCondition(false_condition, next_loader)

        loader.load(value)

        verify(next_loader, times=0).load(value)

    def _givenConditionReturning(self, value: bool):
        condition = mock(Condition)
        when(condition).check(...).thenReturn(value)

        return condition

    def _givenLoader(self):
        return mock(Loader, strict=False)


class LoaderListTest(unittest.TestCase):
    def test_itPushesToAllContainedSinks(self):
        value = 4.234
        loader1 = self._givenLoader()
        loader2 = self._givenLoader()
        loader_list = LoaderList([loader1, loader2])

        loader_list.load(value)

        verify(loader1).load(value)
        verify(loader2).load(value)

    def test_itReturnsSubLoadersResults(self):
        value = 4.234
        loader1 = self._givenLoaderReturning(VALUE_1)
        loader2 = self._givenLoaderReturning(VALUE_2)
        loader_list = LoaderList([loader1, loader2])

        returned_values = loader_list.load(value)

        self.assertEqual(returned_values, [VALUE_1, VALUE_2])

    def _givenLoader(self):
        return mock(Loader, strict=False)

    def _givenLoaderReturning(self, value: float):
        loader = mock(Loader)
        when(loader).load(...).thenReturn(value)

        return loader


class LoaderListUntypedTest(unittest.TestCase):
    def test_itPushesToAllContainedSinks(self):
        value = 4.234
        loader1 = self._givenLoader()
        loader2 = self._givenLoader()
        loader_list = LoaderList([loader1, loader2])

        loader_list.load(value)

        verify(loader1).load(value)
        verify(loader2).load(value)

    def test_itReturnsSubLoadersResults(self):
        value = 4.234
        loader1 = self._givenLoaderReturning(VALUE_1)
        loader2 = self._givenLoaderReturning(VALUE_2)
        loader_list = LoaderListUntyped([loader1, loader2])

        returned_values = loader_list.load(value)

        self.assertEqual(returned_values, [VALUE_1, VALUE_2])

    def _givenLoader(self):
        return mock(Loader, strict=False)

    def _givenLoaderReturning(self, value: float):
        loader = mock(Loader)
        when(loader).load(...).thenReturn(value)

        return loader


class ToStringTest(unittest.TestCase):
    def test_itTransformsToString(self):
        loader = ToString()

        returned_value = loader.load(VALUE_1)

        self.assertEqual(returned_value, str(VALUE_1))


class BufferTest(unittest.TestCase):
    def test_itAccumulatesItemsInLists(self):
        loader = Buffer(size=2)

        returned_items1 = loader.load(VALUE_1)
        returned_items2 = loader.load(VALUE_2)
        returned_items3 = loader.load(VALUE_2)

        self.assertEqual(returned_items1, None)
        self.assertEqual(returned_items2, [VALUE_1, VALUE_2])
        self.assertEqual(returned_items3, None)


class PutToQueueTest(unittest.TestCase):
    def test_itPushesToQueue(self):
        queue = Queue(multiprocessing.Queue())
        loader = PutToQueue(queue, strategy=PutBlocking())

        loader.load(VALUE_1)
        loader.load(VALUE_2)

        self.assertEqual(queue.get(), VALUE_1)
        self.assertEqual(queue.get(), VALUE_2)
