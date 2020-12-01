import subprocess
import atexit
from config_utils import getWorkers

workers = getWorkers("config.json")

processes = []
for worker in workers.values():
    p = subprocess.Popen(["python3", "./worker.py", str(worker.port), str(worker.id)])
    processes.append(p)


def cleanup():
    for p_ in processes:  # list of your processes
        p_.send_signal(subprocess.signal.SIGINT)
    print("cleanup done")


atexit.register(cleanup)

input("Press enter to stop workers")
