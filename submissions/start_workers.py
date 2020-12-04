import subprocess
import atexit
import json
import threading
from typing import List


# master_utils/worker.py
class Worker:
    """Stores worker details as well as a semaphore
    counting the number of slots left

    :param id: worker id
    :param host: hostname or IP of worker
    :param port: port of worker
    :num_slots: total number of slots
    """

    def __init__(self, id_, host, port, num_slots):
        self.id = id_
        self.host = host
        self.port = port
        self.totalSlots = num_slots
        self.slots = threading.BoundedSemaphore(num_slots)
        self.free_slots = num_slots
        self.lock = threading.Lock()

    def delegateTask(self):
        """Decrements the number of available slots by one
        (blocks until a slot is available)
        """
        self.lock.acquire()
        ret = self.slots.acquire()
        self.free_slots -= 1
        self.lock.release()
        return ret

    def finishTask(self):
        """Increments the number of available slots"""
        self.lock.acquire()
        self.slots.release()
        self.free_slots += 1
        self.lock.release()

    def __repr__(self):
        return (
            f"<Worker id={self.id}"
            f" {self.host}:{self.port}"
            f" slots={self.totalSlots}>"
        )


# config_utils/__init__.py
def getWorkers(filename: str) -> List:
    workers = {}
    with open(filename, "rt") as f:
        config = json.load(f)
        for worker in config["workers"]:
            host = worker["host"] if "host" in worker else "127.0.0.1"
            port = worker["port"]
            workers[host, port] = Worker(
                worker["worker_id"], host, port, worker["slots"]
            )
    return workers


workers = getWorkers("config.json")

processes = []
for worker in workers.values():
    p = subprocess.Popen(["python3", "./worker.py", str(worker.port), str(worker.id)])
    processes.append(p)


def cleanup():
    for p_ in processes:  # list of your processes
        p_.send_signal(subprocess.signal.SIGINT)
    print("cleanup done")


atexit.register(cleanup)

input("Press enter to stop workers")
