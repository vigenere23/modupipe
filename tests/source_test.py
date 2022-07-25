import unittest
from typing import Iterator

from pipeline.exceptions import MaxIterationsReached
from pipeline.mapper import Mapper
from pipeline.source import MaxIterations, Source

VALUE = 3.546
MAPPED_VALUE = 6.394


class FakeSource(Source[float]):
    def get(self) -> Iterator[float]:
        yield VALUE
        yield VALUE


class FakeMapper(Mapper[float, float]):
    def map(self, _: Iterator[float]) -> Iterator[float]:
        yield MAPPED_VALUE
        yield MAPPED_VALUE


class MaxIterationsTest(unittest.TestCase):
    def test_itStopsAfterMaxIterations(self):
        source = MaxIterations(FakeSource(), nb_iterations=1)
        items = source.get()

        next(items)

        with self.assertRaises(MaxIterationsReached):
            next(items)

    def test_givenMaxIterationsSameAsNumberOfSourceItems_itDoesNotStop(self):
        source = MaxIterations(FakeSource(), nb_iterations=2)
        items = source.get()

        next(items)
        next(items)


class MappedSourceTest(unittest.TestCase):
    def setUp(self) -> None:
        pass
