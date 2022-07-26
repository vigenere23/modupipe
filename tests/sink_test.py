import multiprocessing
import unittest

from mockito import mock, verify

from modupipe.queue import PutBlocking, Queue
from modupipe.sink import QueueSink, Sink, SinkList

VALUE_1 = 439.234
VALUE_2 = 12.682


class SinkListTest(unittest.TestCase):
    def test_itPushesToAllContainedSinks(self):
        value = 4.234
        sink1 = self._givenSink()
        sink2 = self._givenSink()
        sink_list = SinkList([sink1, sink2])

        sink_list.receive(value)

        verify(sink1).receive(value)
        verify(sink2).receive(value)

    def _givenSink(self):
        return mock(Sink, strict=False)


class QueueSinkTest(unittest.TestCase):
    def test_itPushesToQueue(self):
        queue = Queue(multiprocessing.Queue())
        sink = QueueSink(queue, strategy=PutBlocking())

        sink.receive(VALUE_1)
        sink.receive(VALUE_2)

        self.assertEqual(queue.get(), VALUE_1)
        self.assertEqual(queue.get(), VALUE_2)
