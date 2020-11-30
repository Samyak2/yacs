# YACS

**Yet Another Centralized Scheduler**

### Layout as of now

```
├── config.json
├── master.py
├── master_utils
│   ├── getRequest.py
│   ├── __init__.py
│   ├── recvWorker.py
│   └── sendWorker.py
├── README.md
├── requests.py
├── worker.py
└── worker_utils
```

- `master.py` and `worker.py` are the main worker node and master node control files.
- `master_utils/` houses master node utilities
  - `getRequest.py` handles getting json response from `request.py`. called in one thread and runs infinitely.
  - `recvWorker.py` handles getting messsages from worker nodes. called in one thread and runs infinitely.
  - `sendWorker.py` handles sending task information received from `requests.py`. need to be called somewhere in `master.py`.
- `worker_utils/` houses worker node utilities. yet to be filled sed.

### How to run

Setup master and worker nodes.
```
python3 master.py
python3 worker.py 4000 1
python3 worker.py 4001 2
python3 worker.py 4002 3
```

Now run `requests.py` with the number of requests you want to send to master node. It should send and finish. Let it run in the background. Example:
```
python3 requests.py 100
```

The log file would be generated in the root of the git repo, it's named `master.log`. Master has everything that you'd need. Worker nodes also log but those messages are sent to master, so not to worry about it.

