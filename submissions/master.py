import threading
import logging
import datetime
import queue
import sys
from pprint import pprint
from typing import Dict, List
import json
import socket
import random
from dataclasses import dataclass, asdict
from typing import Tuple


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

# master_utils/scheduler.py
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
        for worker_ in self.workers.values():
            worker_.lock.acquire()
            logging.info("acquiring lock of worker %d", worker_.id)

    def releaseWorkerLocks(self):
        for worker_ in self.workers.values():
            worker_.lock.release()
            logging.info("releasing lock of worker %d", worker_.id)


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


# read configuration
workers = getWorkers("config.json")
pprint(workers)

# Initialize schedulers
scheduler = RandomScheduler(workers)
if len(sys.argv) > 1:
    scAlgo = sys.argv[1]
    scheduler = None
    if scAlgo == "rr":
        scheduler = RoundRobinScheduler(workers)
    elif scAlgo == "ll":
        scheduler = LeastLoaded(workers)


logFile = f"master_{scheduler.name}.log"


class CustomFormatter(logging.Formatter):
    """To get milliseconds in log"""

    converter = datetime.datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s.%03d" % (t, record.msecs)
        return s


logFormatter = CustomFormatter(
    fmt="%(asctime)s: %(message)s", datefmt="%Y-%m-%dT%H:%M:%S.%f%z"
)
logFileHandler = logging.FileHandler(logFile, mode="w")
logFileHandler.setFormatter(logFormatter)
logStreamHandler = logging.StreamHandler()
logStreamHandler.setFormatter(logFormatter)
logHandlers = [logFileHandler, logStreamHandler]
logging.basicConfig(level=logging.INFO, handlers=logHandlers)

# message queues
workerMessages = queue.Queue()
taskQueue = queue.Queue()

# dictionary to associate map task with corresponding reduce task
jobStore: Dict[str, Dict[str, int]] = dict()

# host and port details
host = "localhost"
clientPort = 5000
recvWorkerPort = 5001
sendWorkerPort = 4000

#job_utils/task.py
@dataclass(eq=True, frozen=True)
class Task:
    """Class to store details of each task
    regardless of type
    """

    job_id: str
    task_id: str
    duration: int


@dataclass
class WorkerMessage:
    """Class to store messages from worker"""

    addr: Tuple[str, int]
    message: str
    task_id: int
    job_id: int
    w_id: int


# master_utils/sendWorker.py
def sendWorkerData(worker: Worker, query: dict):
    """
    Sends job query to a specific worker node
    Parameters:
        worker: Worker object
        query: query to be sent
    """
    logging.info("Connecting to worker %s", worker)
    worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    worker_socket.connect((worker.host, worker.port))
    worker_socket.sendall(json.dumps(query).encode())
    worker_socket.close()
    logging.info("Sent data to worker")


# master_utils/getRequest.py
@dataclass
class Query:
    """Class to store client query data"""

    job_id: str
    map_tasks: List[Task]
    reduce_tasks: List[Task]

    def get_tasks(self):
        """A generator to return all available tasks
        in the format (mapper task, reduce task)
        """
        for map_task in self.map_tasks:
            yield map_task


def makeQuery(data):
    """Converts dictionary from client query to a Query object"""
    map_tasks = [Task(data["job_id"], **a) for a in data["map_tasks"]]
    reduce_tasks = [Task(data["job_id"], **a) for a in data["reduce_tasks"]]
    return Query(data["job_id"], map_tasks, reduce_tasks)


def getRequestData(
    host, port, taskQueue: queue.Queue, jobStore: Dict[str, Dict[str, set]]
):
    """
    Gets requests from requests.py
    Parameters:
        host: host ip/name
        port: port to connect with
        queries: queue to store the job queries from requests.py
    """
    clientRequests = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientRequests.bind((host, port))
    clientRequests.listen()
    while True:
        conn, addr = clientRequests.accept()
        with conn:
            logging.info("Connected by %s", addr)
            data = conn.recv(1024)
            if not data:
                return
            data = data.decode("utf-8")
            data = json.loads(data)
            query = makeQuery(data)
            logging.info(
                "NEW_JOB: Got query with job id %s JOB %s", query.job_id, asdict(query)
            )
            jobStore[query.job_id] = dict(
                map_tasks=set(m.task_id for m in query.map_tasks),
                reduce_tasks=query.reduce_tasks,
            )
            for map_task in query.get_tasks():
                taskQueue.put(map_task)


def processTaskQueue(taskQueue: queue.Queue, scheduler):
    """Processes each task in the queue and delegates them
    to the correct worker using given scheduler
    """
    while True:
        task: Task = taskQueue.get()
        worker: Worker = scheduler.getNext()
        logging.info("RUN_TASK: running task %s on worker %s", task.task_id, worker.id)
        worker.delegateTask()
        sendWorkerData(worker, asdict(task))



# master_utils/recvWorker.py
def recvFromWorker(host, port, message):
    """
    Constantly listen and recieve data from worker. Run in a thread.
    Parameters:
        host: hostname/ip
        port: listening port from all workers
        message: a queue that stores all messages from worker nodes
    """
    logging.info("Started listening for messages from worker nodes")
    worker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    worker.bind((host, port))
    worker.listen()
    while True:
        conn, addr = worker.accept()
        with conn:
            logging.info("Connected by %s", addr)
            data = conn.recv(1024)
            if not data:
                return
            data = json.loads(data.decode())
            data["addr"] = tuple(data["addr"])
            msg = WorkerMessage(**data)
            message.put(msg)


def processWorkerMessage(
    workerMessages: queue.Queue,
    taskQueue: queue.Queue,
    workers: Dict[Tuple, Worker],
    jobStore: Dict[str, Dict[str, set]],
):
    """Processes all data added to the worker message queue

    Removes task when done
    """
    while True:
        msg: WorkerMessage = workerMessages.get()
        print("WORKER ID : ", msg)
        logging.info("TASK_DONE: Completed task %s on worker %s", msg.task_id, msg.w_id)
        if msg.task_id is not None:
            workers[msg.addr].finishTask()
            if msg.job_id in jobStore:
                if msg.task_id in jobStore[msg.job_id]["map_tasks"]:
                    jobStore[msg.job_id]["map_tasks"].remove(msg.task_id)
                if len(jobStore[msg.job_id]["map_tasks"]) == 0:
                    for red_task in jobStore[msg.job_id]["reduce_tasks"]:
                        taskQueue.put(red_task)
                    jobStore.pop(msg.job_id)



# start master threads
logging.info("Starting client requests thread.")
clientThread = threading.Thread(
    target=getRequestData, args=(host, clientPort, taskQueue, jobStore)
)
processQueueThread = threading.Thread(
    target=processTaskQueue, args=(taskQueue, scheduler)
)

# worker threads
workerThread = threading.Thread(
    target=recvFromWorker, args=(host, recvWorkerPort, workerMessages)
)
workerMsgThread = threading.Thread(
    target=processWorkerMessage, args=(workerMessages, taskQueue, workers, jobStore)
)

clientThread.start()
processQueueThread.start()
workerThread.start()
workerMsgThread.start()

clientThread.join()
processQueueThread.join()
workerThread.join()
workerMsgThread.join()
