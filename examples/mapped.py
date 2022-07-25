from pipeline.mapper import Buffer, ToString
from pipeline.runnable import Pipeline
from pipeline.sink import Printer
from pipeline.source import RandomSource

source = RandomSource() + ToString[float]() + Buffer[str](size=5)
sink = Printer()

pipeline = Pipeline(source, sink)

pipeline.run()
