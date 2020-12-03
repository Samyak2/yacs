import json
import logging
import socket
import queue
from typing import Dict, Tuple

from master_utils.worker import Worker
from job_utils.task import WorkerMessage


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
            data = conn.recv(1024)
            if not data:
                return
            data = json.loads(data.decode())
            data["addr"] = tuple(data["addr"])
            msg = WorkerMessage(**data)
            message.put(msg)


def processWorkerMessage(
    workerMessages: queue.Queue,
    taskQueue: queue.Queue,
    workers: Dict[Tuple, Worker],
    jobStore: Dict[str, Dict[str, set]],
):
    """Processes all data added to the worker message queue

    Removes task when done
    """
    while True:
        msg: WorkerMessage = workerMessages.get()
        logging.info("TASK_DONE: Completed task %s on worker %s", msg.task_id, msg.w_id)
        if msg.task_id is not None:
            workers[msg.addr].finishTask()
            if msg.job_id in jobStore:
                if msg.task_id in jobStore[msg.job_id]["map_tasks"]:
                    jobStore[msg.job_id]["map_tasks"].remove(msg.task_id)
                if len(jobStore[msg.job_id]["map_tasks"]) == 0:
                    for red_task in jobStore[msg.job_id]["reduce_tasks"]:
                        taskQueue.put(red_task)
                    jobStore.pop(msg.job_id)
