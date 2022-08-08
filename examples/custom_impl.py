from typing import Iterator, TypeVar

from modupipe.extractor import Extractor
from modupipe.loader import IdentityLoader
from modupipe.mapper import Mapper, PushTo
from modupipe.runnable import FullPipeline

T = TypeVar("T")


class Incremental(Extractor[int]):
    def extract(self) -> Iterator[int]:
        i = 0

        while True:
            yield i
            i += 1


class DivideBy(Mapper[int, float]):
    def __init__(self, divider: int) -> None:
        self.divider = divider

    def map(self, items: Iterator[int]) -> Iterator[float]:
        for item in items:
            yield item / self.divider


class CustomPrinter(IdentityLoader[T]):
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix

    def load(self, item: T) -> T:
        print(f"{self.prefix}{item}")
        return item


extractor = Incremental() + DivideBy(2) + PushTo(CustomPrinter(prefix="Value is : "))

pipeline = FullPipeline(extractor)

pipeline.run()
