import json
from typing import List
from master_utils.worker import Worker


def getWorkers(filename: str) -> List:
    workers = {}
    with open(filename, "rt") as f:
        config = json.load(f)
        for worker in config["workers"]:
            host = worker["host"] if "host" in worker else "127.0.0.1"
            port = worker["port"]
            workers[host, port] = Worker(
                worker["worker_id"], host, port, worker["slots"]
            )
    return workers
