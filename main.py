from pipeline.mapper import Buffer, Divide, ToChars, ToString
from pipeline.sink import Print
from pipeline.source import RandomSource
from pipeline.stream import Stream

source = RandomSource(nb_iterations=10)
mapper = (
    Divide[float](divider=4)
    .with_next(ToString())
    .with_next(ToChars())
    .with_next(Buffer(size=5))
)
sink = Print()

stream = Stream(source, mapper, sink)

stream.start()
