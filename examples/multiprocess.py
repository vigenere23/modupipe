import multiprocessing

from modupipe.extractor import GetFromQueue, Random
from modupipe.mapper import Print, PutToQueue
from modupipe.queue import GetBlocking, PutBlocking, Queue
from modupipe.runnable import FullPipeline, MultiProcess


def pipeline1(queue: Queue[float]):
    extractor = Random() + PutToQueue(queue, strategy=PutBlocking())

    pipeline = FullPipeline(extractor)

    return pipeline


def pipeline2(queue: Queue[float]):
    extractor = GetFromQueue[float](queue, GetBlocking()) + Print()

    pipeline = FullPipeline(extractor)

    return pipeline


queue = Queue[float](multiprocessing.Queue())
pipelines = MultiProcess([pipeline1(queue), pipeline2(queue)])

pipelines.run()
