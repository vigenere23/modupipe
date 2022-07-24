from pipeline.mapper import Buffer, Divide, ToChars, ToString
from pipeline.base import Pipeline
from pipeline.sink import Print
from pipeline.source import MappedSource, RandomSource

source = RandomSource(nb_iterations=10)
mapper = (
    Divide[float](divider=4)
    .with_next(ToString())
    .with_next(ToChars())
    .with_next(Buffer(size=5))
)
mapped_source = MappedSource(source, mapper)
sink = Print()

pipeline = Pipeline(source, sink)

pipeline.run()
