# YACS

**Yet Another Centralized Scheduler**

# Layout

## Code

 - [`master.py`](./master.py) and `master_utils`(./master_utils)
 - [`worker.py`](./worker.py)
 - [`config_utils`](./config_utils): utilities related to reading the `config.json` file
    which defines workers.
 - [`job_utils`](./job_utils): definitions of tasks, jobs and messages shared by master and workers.
 - [`extras`](./extras): extra scripts such as config generator, sending requests to master, etc.
 - [`start_workers`](./start_workers.py): starts all workers defined in `config.json` locally with the
    correct ports. Workers are stopped once the script exits.

## Documentation

[Documentation](./docs): report, architecture

## Analysis

[Analysis](./analysis)

# How to run

 - Edit the `config.json` file with correct worker details. Or [generate one](./extras/config_gen.py).

 - First the master
    ```
    python3 master.py # uses the random scheduler
    ```
    OR 
    ```
    python3 master.py rr # uses the round robin scheduker
    ```
    OR
    ```
    python3 master.py ll # uses the least loaded scheduler
    ```

 - Now the worker
    ```
    python3 start_workers.py # uses the config.json
    ```

 - Now run `extras/requests.py` with the number of requests you want to send to master node. It should send and finish. Let it run in the background. Example:
    ```
    python3 requests.py 100
    ```

    For more customized testing, `requests_eval.py`
    ```
    python3 requests_eval.py # custom task generated
    ```

# Analysis

Copy over the required logs to `analysis/`.

```
cd analysis
python3 yacs_logs_analysis.py # task 1
python3 graph_task2.py # task 2
```

# Non-local workers

The `external-workers` branch has experimental support for non-local workers. Tested on a Raspberry Pi node.
