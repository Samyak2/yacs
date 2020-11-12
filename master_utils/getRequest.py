import socket
import json
import logging

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
            logging.info('Connected by %s', addr)
            data = conn.recv(1024)
            if not data:
                return
            logging.info("Printing data \n %s", data.decode('utf-8'))
            data = data.decode('utf-8')
            data = json.loads(data)
            queries.put(dict(data))
            logging.info("Printing queries \n %s", queries)
