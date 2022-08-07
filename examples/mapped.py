from modupipe.extractor import Random
from modupipe.mapper import Buffer, Print, ToString
from modupipe.runnable import FullPipeline

extractor = Random() + ToString() + Buffer(size=5) + Print()

pipeline = FullPipeline(extractor)

pipeline.run()
