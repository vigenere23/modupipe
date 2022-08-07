from random import random
from typing import Iterator

from modupipe.extractor import Extractor
from modupipe.mapper import Print
from modupipe.runnable import FullPipeline, Retry


class FailingSource(Extractor[float]):
    def extract(self) -> Iterator[float]:
        while True:
            value = random()

            if value > 0.5:
                yield value
            else:
                raise Exception("Did not work.")


extractor = FailingSource() + Print()

pipeline = Retry(FullPipeline(extractor), nb_times=5)

pipeline.run()
