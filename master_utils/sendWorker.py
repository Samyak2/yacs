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
    try:
        worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        worker_socket.connect((worker.host, worker.port))
        worker_socket.sendall(json.dumps(query).encode())
        worker_socket.close()
        return True
    except Exception as e:
        logging.error("Could not connect to worker %s due to error: %s", worker.id, e)
        return False
