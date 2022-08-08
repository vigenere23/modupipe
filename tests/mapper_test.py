import multiprocessing
import unittest
from typing import Iterable

from mockito import mock, verify, when

from modupipe.base import Condition
from modupipe.loader import Loader
from modupipe.mapper import (
    Buffer,
    ChainedMapper,
    Filter,
    Mapper,
    PushTo,
    PushToAndMap,
    PutToQueue,
    ToString,
)
from modupipe.queue import PutBlocking, Queue

VALUE_1 = 243.2345
VALUE_2 = 39.42
OTHER_VALUE = 923.765


class ChainedMapperTest(unittest.TestCase):
    def test_itChainsMappersTogether(self):
        source_items = iter([VALUE_1, VALUE_2])
        first_mapped_items = iter([str(VALUE_1), str(VALUE_2)])
        second_mapped_items = iter([[VALUE_1], [VALUE_2]])
        mapper1 = self._givenMapperWith(source_items, first_mapped_items)
        mapper2 = self._givenMapperWith(first_mapped_items, second_mapped_items)
        chained_mappers = ChainedMapper(mapper1, mapper2)

        mapped_items = chained_mappers.map(source_items)

        self.assertEqual(mapped_items, second_mapped_items)

    def _givenMapperWith(
        self, receiving_value: Iterable[float], return_value: Iterable[float]
    ):
        mapper = mock(Mapper)
        when(mapper).map(receiving_value).thenReturn(return_value)

        return mapper


class FilterTest(unittest.TestCase):
    def test_itCanFilterIn(self):
        value = VALUE_1
        true_condition = self._givenConditionReturning(True)
        mapper = Filter(true_condition)

        filtered_item = next(mapper.map(iter([value])))

        self.assertEqual(filtered_item, value)

    def test_itCanFilterOut(self):
        value = VALUE_1
        false_condition = self._givenConditionReturning(False)
        mapper = Filter(false_condition)

        with self.assertRaises(StopIteration):
            next(mapper.map(iter([value])))

    def _givenConditionReturning(self, value: bool):
        condition = mock(Condition)
        when(condition).check(...).thenReturn(value)

        return condition


class ToStringTest(unittest.TestCase):
    def test_itTransformsToString(self):
        mapper = ToString()
        items = iter([VALUE_1, VALUE_2])

        mapped_items = list(mapper.map(items))

        expected_items = [str(VALUE_1), str(VALUE_2)]
        self.assertEqual(mapped_items, expected_items)


class BufferTest(unittest.TestCase):
    def test_itAccumulatesItemsInLists(self):
        mapper = Buffer(size=2)
        items = iter([VALUE_1, VALUE_2, VALUE_1])

        mapped_items = next(mapper.map(items))

        expected_items = [VALUE_1, VALUE_2]
        self.assertEqual(mapped_items, expected_items)


class PutToQueueTest(unittest.TestCase):
    def test_itPushesToQueue(self):
        queue = Queue(multiprocessing.Queue())
        mapper = PutToQueue(queue, strategy=PutBlocking())

        list(mapper.map(iter([VALUE_1, VALUE_2])))

        self.assertEqual(queue.get(), VALUE_1)
        self.assertEqual(queue.get(), VALUE_2)


class PushToTest(unittest.TestCase):
    def test_itPushesToLoader(self):
        loader = self._givenLoader()
        mapper = PushTo(loader)

        list(mapper.map(iter([VALUE_1, VALUE_2])))

        verify(loader, inorder=True).load(VALUE_1)
        verify(loader, inorder=True).load(VALUE_2)

    def test_itReturnsTheInputValues(self):
        loader = self._givenLoader()
        mapper = PushTo(loader)

        mapped_values = list(mapper.map(iter([VALUE_1, VALUE_2])))

        self.assertEqual(mapped_values, [VALUE_1, VALUE_2])

    def _givenLoader(self):
        return mock(Loader, strict=False)


class PushToAndMapTest(unittest.TestCase):
    def test_itPushesToLoader(self):
        loader = self._givenLoader()
        mapper = PushToAndMap(loader)

        list(mapper.map(iter([VALUE_1, VALUE_2])))

        verify(loader, inorder=True).load(VALUE_1)
        verify(loader, inorder=True).load(VALUE_2)

    def test_itReturnsTheLoaderOutputValues(self):
        loader = self._givenLoaderReturning(OTHER_VALUE)
        mapper = PushToAndMap(loader)

        mapped_values = list(mapper.map(iter([VALUE_1, VALUE_2])))

        self.assertEqual(mapped_values, [OTHER_VALUE, OTHER_VALUE])

    def _givenLoader(self):
        return mock(Loader, strict=False)

    def _givenLoaderReturning(self, value: float):
        loader = mock(Loader)
        when(loader).load(...).thenReturn(value)

        return loader
