from typing import Dict, Tuple
import time
import random
import logging

from master_utils.worker import Worker

POLLING_TIME = 0.1


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

    name = "Random"

    def getNext(self):
        for worker_ in self.workers.values():
            worker_.lock.acquire()
            logging.info("acquiring lock of worker %d", worker_.id)

        found = False
        while not found:
            worker_addr = random.choice(self.worker_keys)
            worker = self.workers[worker_addr]
            if worker.free_slots > 0:
                found = True
            else:
                time.sleep(POLLING_TIME)

        for worker_ in self.workers.values():
            worker_.lock.release()
            logging.info("releasing lock of worker %d", worker_.id)

        return worker


class RoundRobinScheduler(Scheduler):
    """
    A scheduler that cycles through all
    the workers. It assigns a task a worker
    and goes on to the next.
    """
    name = "Round Robin"

    def getNext(self):
        for worker_ in self.workers.values():
            worker_.lock.acquire()
            logging.info("acquiring lock of worker %d", worker_.id)

        found = False
        while not found:
            worker_addr = self.worker_keys[self.current]
            worker = self.workers[worker_addr]
            if worker.free_slots > 0:
                found = True
            else:
                time.sleep(POLLING_TIME)
            self.current = (self.current + 1) % self.num

        for worker_ in self.workers.values():
            worker_.lock.release()
            logging.info("releasing lock of worker %d", worker_.id)

        return worker


class LeastLoaded(Scheduler):
    name = "LeastLoaded"

    def getNext(self):
        least_worker = None
        most_free_slots = 0
        for worker_ in self.workers.values():
            worker_.lock.acquire()
            if worker_.free_slots > most_free_slots:
                least_worker = worker_
                most_free_slots = worker_.free_slots
            logging.info("acquiring lock of worker %d", worker_.id)

        for worker_ in self.workers.values():
            worker_.lock.release()
            logging.info("releasing lock of worker %d", worker_.id)

        return least_worker
