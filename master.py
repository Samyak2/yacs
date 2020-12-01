import threading
import logging
import queue
from pprint import pprint
import time

from master_utils.getRequest import getRequestData, processRequestData
from master_utils.recvWorker import recvFromWorker, processWorkerMessage
from config_utils import getWorkers
from master_utils.scheduler import RandomScheduler

logFile = "master.log"

logging.basicConfig(
    filename=logFile,
    filemode='a',
    format="%(asctime)s: %(message)s", 
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S%z"
)

# read configuration
workers = getWorkers("config.json")
pprint(workers)

# message queues
queries = queue.Queue()
workerMessages = queue.Queue()

# host and port details
host = "localhost"
clientPort = 5000
recvWorkerPort = 5001
sendWorkerPort = 4000

# Initialize schedulers
# TODO: take user input for this
scheduler = RandomScheduler(workers)

# start master threads
logging.info("Starting client requests thread.")
clientThread = threading.Thread(target=getRequestData, args=(host, clientPort, queries))
clientMsgThread = threading.Thread(target=processRequestData, args=(queries, scheduler))

# worker threads
workerThread = threading.Thread(
    target=recvFromWorker, args=(host, recvWorkerPort, workerMessages)
)
workerMsgThread = threading.Thread(
    target=processWorkerMessage, args=(workerMessages, workers)
)

clientThread.start()
clientMsgThread.start()
workerThread.start()
workerMsgThread.start()
clientThread.join()
clientMsgThread.join()
workerThread.join()
workerMsgThread.join()
