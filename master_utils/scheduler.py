from typing import Dict, Tuple
import time
import random

from master_utils.worker import Worker

POLLING_TIME = 1


class Scheduler:
    """General class for a scheduler"""

    def __init__(self, workers: Dict[Tuple, Worker]):
        self.workers = workers
        self.worker_keys = list(workers.keys())
        self.num = len(workers)

    def getNext(self):
        """Returns next worker to schedule on"""
        raise NotImplementedError


class RandomScheduler(Scheduler):
    """A scheduler that takes a random worker at
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

        return worker
