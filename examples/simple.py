from modupipe.extractor import Random
from modupipe.mapper import Print
from modupipe.runnable import FullPipeline

extractor = Random() + Print()

pipeline = FullPipeline(extractor)

pipeline.run()
