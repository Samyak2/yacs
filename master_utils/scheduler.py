from typing import Dict, Tuple
import time
import random
import logging

from master_utils.worker import Worker

POLLING_TIME = 1


class Scheduler:
    """General class for a scheduler"""

    name = None

    def __init__(self, workers: Dict[Tuple, Worker]):
        self.workers = workers
        self.worker_keys = list(workers.keys())
        self.current = 0
        self.num = len(workers)
        self.slots_left = []
        for key in self.worker_keys:
            self.slots_left.append(self.workers[key].totalSlots)

    def getNext(self):
        """Returns next worker to schedule on"""
        raise NotImplementedError

    def acquireWorkerLocks(self):
        logging.info("acquiring locks of workers")
        for worker_ in self.workers.values():
            worker_.lock.acquire()
        logging.info("acquired locks of workers")

    def releaseWorkerLocks(self):
        logging.info("releasing locks of workers")
        for worker_ in self.workers.values():
            worker_.lock.release()
        logging.info("released locks of workers")


class RandomScheduler(Scheduler):
    """
    A scheduler that takes a random worker at
    a time. If the randomly selected worker is not
    available, wait for POLLING_TIME seconds and try
    again
    """

    name = "Random"

    def getNext(self):
        found = False
        while not found:
            self.acquireWorkerLocks()
            workers_tried = set()
            while len(workers_tried) < self.num:
                if found:
                    break
                worker_addr = random.choice(self.worker_keys)
                worker = self.workers[worker_addr]
                workers_tried.add(worker.id)
                if worker.free_slots > 0:
                    found = True
            self.releaseWorkerLocks()

            if not found:
                time.sleep(POLLING_TIME)

        return worker


class RoundRobinScheduler(Scheduler):
    """
    A scheduler that cycles through all
    the workers. It assigns a task a worker
    and goes on to the next.
    """

    name = "Round_Robin"

    def getNext(self):

        found = False
        while not found:
            self.acquireWorkerLocks()
            for _ in range(self.num):
                if found:
                    break
                worker_addr = self.worker_keys[self.current]
                worker = self.workers[worker_addr]
                if worker.free_slots > 0:
                    found = True
                self.current = (self.current + 1) % self.num
            self.releaseWorkerLocks()

            if not found:
                time.sleep(POLLING_TIME)

        return worker


class LeastLoaded(Scheduler):
    name = "LeastLoaded"

    def getNext(self):

        found = False
        while not found:
            worker = None
            most_free_slots = 0
            self.acquireWorkerLocks()
            for worker_ in self.workers.values():
                if found:
                    break
                if worker_.free_slots > most_free_slots:
                    worker = worker_
                    most_free_slots = worker_.free_slots
            if worker is not None and most_free_slots > 0:
                found = True
            self.releaseWorkerLocks()

            if not found:
                time.sleep(POLLING_TIME)

        return worker
