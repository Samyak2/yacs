import threading
import logging
import datetime
import queue
import sys
from pprint import pprint
from typing import Dict

from master_utils.getRequest import getRequestData, processTaskQueue
from master_utils.recvWorker import recvFromWorker, processWorkerMessage
from master_utils.scheduler import RandomScheduler, RoundRobinScheduler, LeastLoaded
from config_utils import getWorkers
from job_utils.task import Task

# read configuration
workers = getWorkers("config.json")
pprint(workers)

# Initialize schedulers
scheduler = RandomScheduler(workers)
if len(sys.argv) > 1:
    scAlgo = sys.argv[1]
    scheduler = None
    if scAlgo == "rr":
        scheduler = RoundRobinScheduler(workers)
    elif scAlgo == "ll":
        scheduler = LeastLoaded(workers)


logFile = f"master_{scheduler.name}.log"


class CustomFormatter(logging.Formatter):
    """To get milliseconds in log"""

    converter = datetime.datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s.%03d" % (t, record.msecs)
        return s


logFormatter = CustomFormatter(
    fmt="%(asctime)s: %(message)s", datefmt="%Y-%m-%dT%H:%M:%S.%f%z"
)
logFileHandler = logging.FileHandler(logFile, mode="w")
logFileHandler.setFormatter(logFormatter)
logStreamHandler = logging.StreamHandler()
logStreamHandler.setFormatter(logFormatter)
logHandlers = [logFileHandler, logStreamHandler]
logging.basicConfig(level=logging.INFO, handlers=logHandlers)

# message queues
workerMessages = queue.Queue()
taskQueue = queue.Queue()

# dictionary to associate map task with corresponding reduce task
mapRedMap: Dict[str, Task] = dict()

# host and port details
host = "localhost"
clientPort = 5000
recvWorkerPort = 5001
sendWorkerPort = 4000

# start master threads
logging.info("Starting client requests thread.")
clientThread = threading.Thread(
    target=getRequestData, args=(host, clientPort, taskQueue, mapRedMap)
)
processQueueThread = threading.Thread(
    target=processTaskQueue, args=(taskQueue, scheduler)
)

# worker threads
workerThread = threading.Thread(
    target=recvFromWorker, args=(host, recvWorkerPort, workerMessages)
)
workerMsgThread = threading.Thread(
    target=processWorkerMessage, args=(workerMessages, taskQueue, workers, mapRedMap)
)

clientThread.start()
processQueueThread.start()
workerThread.start()
workerMsgThread.start()

clientThread.join()
processQueueThread.join()
workerThread.join()
workerMsgThread.join()
