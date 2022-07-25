from pipeline.runnable import Pipeline
from pipeline.sink import Print
from pipeline.source import RandomSource

source = RandomSource()
sink = Print()

pipeline = Pipeline(source, sink)

pipeline.run()
