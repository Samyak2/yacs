import socket
import json
import logging

def sendWorkerData(host, port, query):
    """
    Sends job query to a specific worker node at a given port
    Parameters:
        host: hostname / host ip
        port: port of the worker node
        query: query to be sent
    """
    worker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    worker.connect((host, port))
    worker.sendall(str(query).encode())
