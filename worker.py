import socket
import logging
import sys
import json
import threading
from pprint import pformat
import time
from dataclasses import asdict
from job_utils.task import Task
from job_utils.task import WorkerMessage

logging.basicConfig(
    format="%(asctime)s: %(message)s", level=logging.INFO, datefmt="%Y-%m-%dT%H:%M:%S%z"
)

port = int(sys.argv[1])
w_id = int(sys.argv[2])
host = "127.0.0.1" if len(sys.argv) < 4 else sys.argv[3]
responsePort = 5001
responseHost = "127.0.0.1"

logging.info("Starting worker %d on port %d", w_id, port)

master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
master.bind((host, port))


def waste_time(task: Task):
    time_left = task.duration
    time.sleep(task.duration)
    logging.info("Completed task %s on worker %d", task.task_id, w_id)
    msg = WorkerMessage(
        addr=[host, port],
        message="Completed task {} on worker {}".format(task.task_id, w_id),
        task_id=task.task_id,
        w_id=w_id,
        job_id=task.job_id,
    )
    msg = json.dumps(asdict(msg))
    master_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_send.connect((responseHost, responsePort))
    master_send.sendall(msg.encode())
    master_send.close()


master.listen()

while True:
    conn, addr = master.accept()
    with conn:
        logging.info("Connected by %s", addr)
        if addr[0] != responseHost:
            responseHost = addr[0]
        print(responseHost, responsePort)
        data = conn.recv(1024)
        if not data:
            continue
        data = json.loads(data)
        task = Task(**data)
        logging.info("Got task: %s", pformat(task))
        # task_id, duration
        logging.info("starting task %s on worker %d", task, w_id)
        thread = threading.Thread(target=waste_time, args=(task,))
        thread.start()
