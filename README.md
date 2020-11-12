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
