# Extra scripts

 - [`calc_latency.py`](./calc_latency.py): used to calculate difference between two timestamps from the logs.
 - [`config_gen.py`](./calc_latency.py): used to generate `config.json` for an arbitrary number of nodes.
    Usage: `python config_gen.py <number of nodes>`. Each node has 5 slots and port numbers start from 4000.
    The generated config file can be used by [start_workers.py](/start_workers.py) to start the correct number
    of workers.
 - [`requests.py`](./requests.py): sends job requests to master.
 - [`requests_eval.py`](./requests_eval.py): sends job requests to master. Provides more control over
    parameters. Used for evaluation.
