import socket
import json
import logging
from master_utils.worker import Worker


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
