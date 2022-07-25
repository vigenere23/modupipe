from modupipe.mapper import Buffer, ToString
from modupipe.runnable import Pipeline
from modupipe.sink import Printer
from modupipe.source import RandomSource

source = RandomSource() + ToString[float]() + Buffer[str](size=5)
sink = Printer()

pipeline = Pipeline(source, sink)

pipeline.run()
