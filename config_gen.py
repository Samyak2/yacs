import json
import sys

if len(sys.argv) < 2:
    num_workers = 10
else:
    num_workers = int(sys.argv[1])

workers = []
num_slots = 5
for i in range(1, num_workers + 1):
    workers.append({
        "worker_id": i,
        "slots": num_slots,
        "port": 4000+i
    })
with open("./config.json", "wt") as f:
    json.dump({
        "workers": workers
    }, f, indent=4)
