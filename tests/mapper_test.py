import unittest
from typing import Iterable

from mockito import mock, when

from modupipe.mapper import Buffer, Mapper, Next, ToString

VALUE_1 = 243.2345
VALUE_2 = 39.42


class NextTest(unittest.TestCase):
    def test_itChainsMappersTogether(self):
        source_items = iter([VALUE_1, VALUE_2])
        first_mapped_items = iter([str(VALUE_1), str(VALUE_2)])
        second_mapped_items = iter([[VALUE_1], [VALUE_2]])
        mapper1 = self._givenMapperWith(source_items, first_mapped_items)
        mapper2 = self._givenMapperWith(first_mapped_items, second_mapped_items)
        chained_mappers = Next(mapper1, mapper2)

        mapped_items = chained_mappers.map(source_items)

        self.assertEqual(mapped_items, second_mapped_items)

    def _givenMapperWith(
        self, receiving_value: Iterable[float], return_value: Iterable[float]
    ):
        mapper = mock(Mapper)
        when(mapper).map(receiving_value).thenReturn(return_value)

        return mapper


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
