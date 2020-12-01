import socket
import json
import logging
import queue
from typing import List, Dict
from dataclasses import dataclass, asdict

from master_utils.sendWorker import sendWorkerData
from master_utils.worker import Worker
from job_utils.task import Task


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
        for map_task, red_task in zip(self.map_tasks, self.reduce_tasks):
            yield map_task, red_task


def makeQuery(data):
    """Converts dictionary from client query to a Query object"""
    map_tasks = [Task(**a) for a in data["map_tasks"]]
    reduce_tasks = [Task(**a) for a in data["reduce_tasks"]]
    return Query(data["job_id"], map_tasks, reduce_tasks)


def getRequestData(host, port, taskQueue: queue.Queue, mapRedMap: Dict[str, Task]):
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
            logging.info("NEW_JOB: Got query with job id %s", query.job_id)
            for map_task, red_task in query.get_tasks():
                mapRedMap[map_task.task_id] = red_task
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
