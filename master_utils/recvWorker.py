import json
import logging
import socket

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
            logging.info('Connected by %s', addr)
            data = conn.recv(1024)
            if not data:
                return
            data = json.loads(data.decode())
            logging.info("Got data %s", data)
            message.put(dict(data))
