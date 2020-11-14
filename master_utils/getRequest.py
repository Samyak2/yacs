import socket
import json
import logging
import queue
from pprint import pformat
from typing import List
from dataclasses import dataclass, asdict

from master_utils.sendWorker import sendWorkerData
from job_utils.task import Task


@dataclass
class Query:
    """Class to store client query data"""

    job_id: str
    map_tasks: List[Task]
    reduce_tasks: List[Task]

    def get_tasks(self):
        """A generator to return all available tasks
        in this query
        """
        for task in self.map_tasks:
            yield task
        for task in self.reduce_tasks:
            yield task


def makeQuery(data):
    """Converts dictionary from client query to a Query object"""
    map_tasks = [Task(**a) for a in data["map_tasks"]]
    reduce_tasks = [Task(**a) for a in data["reduce_tasks"]]
    return Query(data["job_id"], map_tasks, reduce_tasks)


def getRequestData(host, port, queries):
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
            logging.info("Got query: %s", pformat(query))
            queries.put(query)


def processRequestData(queries: queue.Queue, scheduler):
    """Processes queries added to the queue and sends
    data to the given worker"""
    while True:
        query: Query = queries.get()
        logging.info("Processing query %s", pformat(query))
        worker = scheduler.getNext()
        for task in query.get_tasks():
            sendWorkerData(worker, asdict(task))
