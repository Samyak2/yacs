import socket
import logging
import sys
import json
from pprint import pformat

from job_utils.task import Task

logging.basicConfig(
    format="%(asctime)s: %(message)s", level=logging.INFO, datefmt="%H:%M:%S"
)
port = int(sys.argv[1])
w_id = int(sys.argv[2])

logging.info("Starting worker %d on port %d", w_id, port)

master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = "localhost"
master.bind((host, port))
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
