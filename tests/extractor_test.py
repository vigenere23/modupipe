import multiprocessing
import unittest
from typing import Iterator, List

from mockito import mock, when

from modupipe.exceptions import MaxIterationsReached
from modupipe.extractor import (
    Extractor,
    ExtractorList,
    GetFromQueue,
    MappedExtractor,
    MaxIterations,
)
from modupipe.mapper import Mapper
from modupipe.queue import GetBlocking, Queue

VALUE_1 = 3.546
VALUE_2 = 2349.234


class ExtractorListTest(unittest.TestCase):
    def test_itIteratesThroughEachExtractor(self):
        items1 = [1, 2, 3]
        items2 = [4, 5, 6]
        extractor1 = self._givenExtractorReturning(items1)
        extractor2 = self._givenExtractorReturning(items2)
        extractor = ExtractorList([extractor1, extractor2])

        items = list(extractor.extract())

        self.assertEqual(items, [(1, 4), (2, 5), (3, 6)])

    def _givenExtractorReturning(self, items: List[float]):
        extractor = mock(Extractor)
        when(extractor).extract().thenReturn(iter(items))

        return extractor


class MaxIterationsTest(unittest.TestCase):
    def test_itStopsAfterMaxIterations(self):
        fail_after = 2
        extractor = MaxIterations(
            self._givenExtractorWith(n_items=fail_after + 1), nb_iterations=fail_after
        )
        items = extractor.extract()

        for _ in range(fail_after):
            next(items)

        with self.assertRaises(MaxIterationsReached):
            next(items)

    def _givenExtractorWith(self, n_items: int):
        extractor = mock(Extractor)
        when(extractor).extract().thenReturn(iter([VALUE_1] * n_items))

        return extractor


class MappedExtractorTest(unittest.TestCase):
    def test_itMapsExtractorValues(self):
        source_items = iter([VALUE_1, VALUE_2])
        mapped_items = [VALUE_1 / 2, VALUE_2 / 2]
        extractor = self._givenExtractorReturning(source_items)
        mapper = self._givenMapperWith(source_items, mapped_items)
        mapped_source = MappedExtractor(extractor, mapper)

        items = list(mapped_source.extract())

        self.assertEqual(items, mapped_items)

    def _givenExtractorReturning(self, source_items: Iterator[float]):
        extractor = mock(Extractor)
        when(extractor).extract().thenReturn(source_items)
        return extractor

    def _givenMapperWith(
        self, source_items: Iterator[float], mapped_items: Iterator[float]
    ):
        mapper = mock(Mapper)
        when(mapper).map(source_items).thenReturn(iter(mapped_items))
        return mapper


class GetFromQueueTest(unittest.TestCase):
    def test_itGetsFromQueue(self):
        queue = Queue(multiprocessing.Queue())
        queue.put(VALUE_1)
        queue.put(VALUE_2)
        extractor = GetFromQueue(queue=queue, strategy=GetBlocking())

        items = extractor.extract()

        self.assertEqual(next(items), VALUE_1)
        self.assertEqual(next(items), VALUE_2)
