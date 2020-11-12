import socket
import threading
import json
import logging
import queue
from master_utils.getRequest import getRequestData
from master_utils.sendWorker import sendWorkerData
from master_utils.recvWorker import recvFromWorker

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

queries = queue.Queue()
workerMessages = queue.Queue()

host = "localhost"
clientPort = 5000
recvWorkerPort = 5001
sendWorkerPort = 4000

logging.info("Starting client requests thread.")
clientThread = threading.Thread(target=getRequestData, args=(host, clientPort, queries))
workerThread = threading.Thread(target=recvFromWorker, args=(host, recvWorkerPort, workerMessages))
clientThread.start()
workerThread.start()
clientThread.join()
workerThread.join()
