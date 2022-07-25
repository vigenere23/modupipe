from pipeline.runnable import Pipeline
from pipeline.sink import Printer
from pipeline.source import RandomSource

source = RandomSource()
sink = Printer()

pipeline = Pipeline(source, sink)

pipeline.run()
