import multiprocessing

from modupipe.queue import GetBlocking, PutBlocking, Queue
from modupipe.runnable import MultiProcess, Pipeline
from modupipe.sink import Printer, QueueSink
from modupipe.source import QueueSource, RandomSource


def pipeline1(queue: Queue[float]):
    source = RandomSource()
    sink = QueueSink(queue, strategy=PutBlocking())

    pipeline = Pipeline(source, sink)

    return pipeline


def pipeline2(queue: Queue[float]):
    source = QueueSource[float](queue, GetBlocking())
    sink = Printer()

    pipeline = Pipeline(source, sink)

    return pipeline


queue = Queue[float](multiprocessing.Queue())
pipelines = MultiProcess([pipeline1(queue), pipeline2(queue)])

pipelines.run()
