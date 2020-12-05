# YACS

**Yet Another Centralized Scheduler**

# How to run

First the master
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
Now the worker
```
python3 start_workers.py # uses the config.json
```

Now run `requests.py` with the number of requests you want to send to master node. It should send and finish. Let it run in the background. Example:
```
python3 requests.py 100
```
For more customized testing, can also use `config_gen.py` and `requests_eval.py`
```
python3 config_gen.py # generates config.json
python3 requests_eval.py # custom task generated
```

Finally the analysis
```
python3 yacs_logs_analysis.py # task 1
python3 graph_task2.py # task 2
```

# 4 file run
Go to the `submissions/` folder and similary run the stuff