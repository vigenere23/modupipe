import multiprocessing
import unittest
from typing import Iterator

from mockito import mock, when

from modupipe.exceptions import MaxIterationsReached
from modupipe.mapper import Mapper
from modupipe.queue import GetBlocking, Queue
from modupipe.source import MappedSource, MaxIterations, QueueSource, Source

VALUE_1 = 3.546
VALUE_2 = 2349.234


class FakeSource(Source[float]):
    def fetch(self) -> Iterator[float]:
        yield VALUE_1
        yield VALUE_2


class MaxIterationsTest(unittest.TestCase):
    def test_itStopsAfterMaxIterations(self):
        fail_after = 2
        source = MaxIterations(
            self._givenSourceWith(n_items=fail_after + 1), nb_iterations=fail_after
        )
        items = source.fetch()

        for _ in range(fail_after):
            next(items)

        with self.assertRaises(MaxIterationsReached):
            next(items)

    def _givenSourceWith(self, n_items: int):
        source = mock(Source)
        when(source).fetch().thenReturn(iter([VALUE_1] * n_items))

        return source


class MappedSourceTest(unittest.TestCase):
    def test_itMapsSourceValues(self):
        source_items = iter([VALUE_1, VALUE_2])
        mapped_items = [VALUE_1 / 2, VALUE_2 / 2]
        source = self._givenSourceReturning(source_items)
        mapper = self._givenMapperWith(source_items, mapped_items)
        mapped_source = MappedSource(source, mapper)

        items = list(mapped_source.fetch())

        self.assertEqual(items, mapped_items)

    def _givenSourceReturning(self, source_items: Iterator[float]):
        source = mock(Source)
        when(source).fetch().thenReturn(source_items)
        return source

    def _givenMapperWith(
        self, source_items: Iterator[float], mapped_items: Iterator[float]
    ):
        mapper = mock(Mapper)
        when(mapper).map(source_items).thenReturn(iter(mapped_items))
        return mapper


class QueueSourceTest(unittest.TestCase):
    def test_itSourcesFromQueue(self):
        queue = Queue(multiprocessing.Queue())
        queue.put(VALUE_1)
        queue.put(VALUE_2)
        source = QueueSource(queue=queue, strategy=GetBlocking())

        items = source.fetch()

        self.assertEqual(next(items), VALUE_1)
        self.assertEqual(next(items), VALUE_2)
