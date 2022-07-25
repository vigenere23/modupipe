from modupipe.runnable import Pipeline
from modupipe.sink import Printer
from modupipe.source import RandomSource

source = RandomSource()
sink = Printer()

pipeline = Pipeline(source, sink)

pipeline.run()
