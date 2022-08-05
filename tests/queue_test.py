import multiprocessing
import queue
import unittest
from abc import ABC, abstractmethod
from typing import TypeVar, Union

from modupipe.queue import Queue

T = TypeVar("T")


class QueueTest(ABC):
    class Base(unittest.TestCase, ABC):
        @abstractmethod
        def givenPythonQueue(
            self,
        ) -> Union["queue.Queue[T]", "multiprocessing.Queue[T]"]:
            pass

        def test_itCanHaveAName(self):
            name = "some queue"
            python_queue = self.givenPythonQueue()
            queue = Queue(python_queue, name=name)

            self.assertEqual(queue.name, name)

        def test_givenNoName_itHasARandomName(self):
            python_queue = self.givenPythonQueue()
            queue = Queue(python_queue)

            self.assertTrue(len(queue.name) != 0)

        def test_itHasALength(self):
            python_queue = self.givenPythonQueue()
            queue = Queue(python_queue)

            self.assertEqual(len(queue), 0)

        def test_whenPuttingItem_lengthIncreases(self):
            python_queue = self.givenPythonQueue()
            queue = Queue(python_queue)

            queue.put(5.4)

            self.assertEqual(len(queue), 1)

        def test_itCanGetItems(self):
            item = 3.9
            python_queue = self.givenPythonQueue()
            queue = Queue(python_queue)

            queue.put(item)

            self.assertEqual(queue.get(), item)

        def test_whenGettingItems_itGetsInSameOrder(self):
            item1 = 3.9
            item2 = 76.4
            python_queue = self.givenPythonQueue()
            queue = Queue(python_queue)

            queue.put(item1)
            queue.put(item2)

            self.assertEqual(queue.get(), item1)
            self.assertEqual(queue.get(), item2)


class SimpleQueueTest(QueueTest.Base):
    def givenPythonQueue(self) -> Union["queue.Queue[T]", "multiprocessing.Queue[T]"]:
        return queue.Queue()


class MultiprocessingQueueTest(QueueTest.Base):
    def givenPythonQueue(self) -> Union["queue.Queue[T]", "multiprocessing.Queue[T]"]:
        return multiprocessing.Queue()
