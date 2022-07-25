import multiprocessing
from typing import List
from pipeline.mapper import Buffer, ToString
from pipeline.queue import GetBlocking, PutBlocking, Queue
from pipeline.runnable import MultiProcess, Pipeline
from pipeline.sink import Print, QueueSink
from pipeline.source import MaxIterations, MappedSource, QueueSource, RandomSource


def pipeline1(queue: Queue[List[str]]):
    source = MaxIterations(RandomSource(), nb_iterations=10)
    mapper = ToString[float]().with_next(Buffer(size=5))
    mapped_source = MappedSource(source, mapper)
    sink = QueueSink(queue, strategy=PutBlocking())

    pipeline = Pipeline(mapped_source, sink)

    return pipeline


def pipeline2(queue: Queue[List[str]]):
    source = QueueSource[List[str]](queue, GetBlocking())
    sink = Print()

    pipeline = Pipeline(source, sink)

    return pipeline


queue = Queue[List[str]](multiprocessing.Queue())
pipelines = MultiProcess([pipeline1(queue), pipeline2(queue)])

pipelines.run()
