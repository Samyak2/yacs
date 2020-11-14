import json
import logging
import socket
import queue
from typing import Dict, Tuple
from dataclasses import dataclass

from master_utils.worker import Worker


@dataclass
class WorkerMessage:
    """Class to store messages from worker"""

    addr: Tuple[str, int]
    task_id: int
    message: str


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
            msg = WorkerMessage(addr, **data)
            message.put(msg)


def processWorkerMessage(workerMessages: queue.Queue, workers: Dict[Tuple, Worker]):
    """Processes all data added to the worker message queue

    Removes task when done
    """
    while True:
        msg = workerMessages.get()
        logging.info("Got data %s", msg)
        if msg.job_id is not None:
            workers[msg.addr].finishTask()
