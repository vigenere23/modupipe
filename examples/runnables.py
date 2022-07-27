from random import random
from typing import Iterator

from modupipe.runnable import Pipeline, Retry
from modupipe.sink import Printer
from modupipe.source import Source


class FailingSource(Source[float]):
    def fetch(self) -> Iterator[float]:
        while True:
            value = random()

            if value > 0.5:
                yield value
            else:
                raise Exception("Did not work.")


source = FailingSource()
sink = Printer()

pipeline = Retry(Pipeline(source, sink), nb_times=5)

pipeline.run()
