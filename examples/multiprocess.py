import multiprocessing

from pipeline.queue import GetBlocking, PutBlocking, Queue
from pipeline.runnable import MultiProcess, Pipeline
from pipeline.sink import Print, QueueSink
from pipeline.source import QueueSource, RandomSource


def pipeline1(queue: Queue[float]):
    source = RandomSource()
    sink = QueueSink(queue, strategy=PutBlocking())

    pipeline = Pipeline(source, sink)

    return pipeline


def pipeline2(queue: Queue[float]):
    source = QueueSource[float](queue, GetBlocking())
    sink = Print()

    pipeline = Pipeline(source, sink)

    return pipeline


queue = Queue[float](multiprocessing.Queue())
pipelines = MultiProcess([pipeline1(queue), pipeline2(queue)])

pipelines.run()
