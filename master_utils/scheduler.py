from typing import Dict, Tuple
import time
import random
from threading import Lock

from master_utils.worker import Worker

POLLING_TIME = 1

mutex = Lock()

class Scheduler:
    """General class for a scheduler"""

    def __init__(self, workers: Dict[Tuple, Worker]):
        self.workers = workers
        self.worker_keys = list(workers.keys())
        self.current = 0
        self.num = len(workers)
        self.slots_left = []
        for key in self.worker_keys:
            self.slots_left.append(self.workers[key].totalSlots)
        print(" Printing workers UwU ", self.workers[self.worker_keys[0]].slots)

    def getNext(self):
        """Returns next worker to schedule on"""
        raise NotImplementedError


class RandomScheduler(Scheduler):
    """
    A scheduler that takes a random worker at
    a time. If the randomly selected worker is not
    available, wait for POLLING_TIME seconds and try
    again
    """

    def getNext(self):
        found = False
        while not found:
            worker_addr = random.choice(self.worker_keys)
            worker = self.workers[worker_addr]
            if worker.slots.acquire(blocking=False):
                worker.slots.release()
                found = True
            time.sleep(POLLING_TIME)
            worker_id = self.worker_keys.index(worker_addr)


        return worker

class RoundRobinScheduler(Scheduler):
    """
    A scheduler that cycles through all 
    the workers. It assigns a task a worker
    and goes on to the next.
    """

    def getNext(self):
        found = False
        while not found:
            worker_addr = self.worker_keys[self.current]
            worker = self.workers[worker_addr]
            if worker.slots.acquire(blocking=False):
                worker.slots.release()
                found = True
            time.sleep(POLLING_TIME)
            self.current = (self.current + 1) % 3

        return worker

class LeastLoaded(Scheduler):

    def getNext(self):
        print("something lol")