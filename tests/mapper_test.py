import unittest

from modupipe.mapper import Buffer, Next, ToString

VALUE_1 = 243.2345
VALUE_2 = 39.42


class NextTest(unittest.TestCase):
    def test_itChainsMappersTogether(self):
        mapper1 = ToString()
        mapper2 = Buffer(size=2)
        chained_mappers = Next(mapper1, mapper2)
        items = iter([VALUE_1, VALUE_2])

        mapped_items = next(chained_mappers.map(items))

        expected_items = [str(VALUE_1), str(VALUE_2)]
        self.assertEqual(mapped_items, expected_items)


class ToStringTest(unittest.TestCase):
    def test_itTransformsToSting(self):
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
