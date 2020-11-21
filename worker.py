import socket
import logging
import sys
import json
import threading
from pprint import pformat
import time
from job_utils.task import Task

logging.basicConfig(
    format="%(asctime)s: %(message)s", level=logging.INFO, datefmt="%H:%M:%S"
)
port = int(sys.argv[1])
w_id = int(sys.argv[2])
responsePort = 5001
responseHost = "localhost"

logging.info("Starting worker %d on port %d", w_id, port)

master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = "localhost"
master.bind((host, port))


def waste_time(task_id, duration):
    while duration != 0:
        time.sleep(1)
        duration -= 1
    logging.info("Completed task %s on worker %d", task_id, w_id)
    response = {}
    response["message"] = "Completed task {} on worker {} %d".format(task_id, w_id)
    response["task_id"] = task_id
    data = json.dumps(response)
    # master.connect((responseHost,responsePort))
    master.sendall(data.encode())


master.listen()

while True:
    conn, addr = master.accept()
    with conn:
        logging.info("Connected by %s", addr)
        data = conn.recv(1024)
        if not data:
            continue
        data = json.loads(data)
        task = Task(**data)
        logging.info("Got task: %s", pformat(task))
        # task_id, duration
        task_id = data["task_id"]
        duration = data["duration"]
        logging.info("starting task %s on worker %d", task_id, w_id)
        thread = threading.Thread(target=waste_time, args=(task_id, duration))
        thread.start()
