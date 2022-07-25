from pipeline.mapper import Buffer, ToString
from pipeline.runnable import Pipeline
from pipeline.sink import Print
from pipeline.source import MappedSource, RandomSource

source = RandomSource()
mapper = ToString[float]().with_next(Buffer(size=5))
mapped_source = MappedSource(source, mapper)
sink = Print()

pipeline = Pipeline(mapped_source, sink)

pipeline.run()
