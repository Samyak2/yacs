import json
import datetime
import re
from collections import defaultdict
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

def workerGraph(filename):
    with open(filename) as f:
        lines = f.readlines()

    jobs = {}
    tasks = {}
    workers = defaultdict(list)

    for line in lines:
        line = line.strip()
        ts = line.split(": ")[0]
        ts = datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")
        if "NEW_JOB" in line:
            new_job = {}
            new_job["start_time"] = ts
            job_dict = eval(line.split(" JOB ", maxsplit=1)[1])
            num_jobs = len(job_dict["map_tasks"]) + len(job_dict["reduce_tasks"])
            new_job["num_jobs"] = num_jobs
            new_job.update(job_dict)
            jobs[new_job["job_id"]] = new_job
        if "RUN_TASK" in line:
            task_id = re.findall(r'(?:\d+_+[a-zA-Z]+\d+)', line[31:])[0]
            w_id = re.findall(r'worker (\d+)', line[31:])[0]
            tasks[task_id] = {"task_id": task_id, "start_time": ts, "w_id": w_id}
            workers[w_id].append(tasks[task_id])
        if "TASK_DONE" in line:
            task_id = re.findall(r'(?:\d+_+[a-zA-Z]+\d+)', line[31:])[0]
            tasks[task_id]["end_time"] = ts

            job_id = task_id.split("_")[0]
            jobs[job_id]["num_jobs"] -= 1
            if jobs[job_id] == 0:
                jobs[job_id]["end_time"] = ts


    x = list(i for i in workers.keys())
    x.sort()

    y = [len(workers[i]) for i in x]

    print(x, y)

    plt.bar(x, y)
    plt.xlabel("Worker ID")
    plt.ylabel("Number of tasks run")
    plt.savefig(filename+"_bar"+".png")
    
    df = pd.DataFrame({}, columns=["Timestamp", *sorted(list(workers.keys()))])
    for task_id, task in tasks.items():
        new_row = {}
        for k in workers.keys():
            new_row[k] = 0
        new_row["Timestamp"] = task["start_time"]
        new_row[task["w_id"]] = 1
        df = df.append(new_row, ignore_index=True)

        new_row = {}
        for k in workers.keys():
            new_row[k] = 0
        new_row["Timestamp"] = task["end_time"]
        new_row[task["w_id"]] = -1
        df = df.append(new_row, ignore_index=True)

    df.set_index("Timestamp", inplace=True)
    df.sort_index(inplace=True)
    # pd.set_option('display.max_rows', df.shape[0]+1)

    for k in workers.keys():
        df[k] = df[k].cumsum(axis=0)
    p = df.plot()
    # display(df)
    plt.xlabel("Time")
    plt.ylabel("Number of tasks running")
    plt.savefig(filename+"_line"+".png")
    fig, ax = plt.subplots(figsize=(20,20))

    figs = sns.heatmap(df.fillna(0),annot=True, fmt="d", linewidths=.5,ax=ax)
    figs.figure.savefig(filename+".png")
    return jobs, tasks, workers

jobs, tasks, workers = workerGraph("./master_Random.log")

jobs, tasks, workers = workerGraph("./master_Round_Robin.log")

jobs, tasks, workers = workerGraph("./master_LeastLoaded.log")

