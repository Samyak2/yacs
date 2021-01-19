import pandas as pd
from datetime import timedelta
import re
import os
import numpy as np
import json
import statistics 
import dateutil.parser
from datetime import datetime
import simplejson
import matplotlib.pyplot as plt
df_tasks = pd.DataFrame(columns=['FILENAME','MEAN','MEDIAN']) 
df_jobs = pd.DataFrame(columns=['FILENAME','MEAN','MEDIAN'])
files = ["master_Random.log", "master_LeastLoaded.log", "master_Round_Robin.log"]

for i in files:
    fileName = i
    print(fileName)
    df=pd.DataFrame(columns=['Type','NEW_JOB_ID','TimeStamp','RUN_TASK_ID','TASK_DONE_ID','MAP_IDS','REDUCE_IDS'])
    with open(fileName,'r') as logFile:
      nj_ttl=-1
      for count,line in enumerate(logFile):
        line = line.strip()
        line = line.replace("'",'"')
        df_dict=dict()
        df_dict['Type'] = [fileName.split('.')[0]]
        
        if 'NEW_JOB: 'in line:
          nj_id =  int(re.findall(r'\b\d+\b', line.split("NEW_JOB: ")[1])[0])    
          ts = line.split(": ")[0]
          nj_ttl = nj_id
          data = eval(line.split("JOB ")[1])
          map_tasks = ''
          reduce_tasks = ''

          for i in data['map_tasks']:
            map_tasks += i['task_id']+','
          for i in data['reduce_tasks']:
            reduce_tasks += i['task_id']+','
          
          df_dict['NEW_JOB_ID'] = [nj_id]
          df_dict['TimeStamp'] = [ts]
          df_dict['RUN_TASK_ID']=['']
          df_dict['TASK_DONE_ID']=['']
          df_dict['MAP_IDS'] = [map_tasks[:-1]]
          df_dict['REDUCE_IDS'] = [reduce_tasks[:-1]]

        elif 'RUN_TASK: ' in line:
          rt_id =  re.findall(r'(?:\d+_+[a-zA-Z]+\d+)', line[31:])[0]
          ts = line.split(": ")[0]
          df_dict['NEW_JOB_ID'] = ['']
          df_dict['TimeStamp'] = [ts]
          df_dict['RUN_TASK_ID']=[rt_id]
          df_dict['TASK_DONE_ID']=['']
          df_dict['MAP_IDS'] = ['']
          df_dict['REDUCE_IDS'] = ['']

        elif 'TASK_DONE: ' in line:
          td_id =  re.findall(r'(?:\d+_+[a-zA-Z]+\d+)', line[31:])[0]
          ts = line.split(": ")[0]
          df_dict['NEW_JOB_ID'] = ['']
          df_dict['TimeStamp'] = [ts]
          df_dict['RUN_TASK_ID']=['']
          df_dict['TASK_DONE_ID']=[td_id]
          df_dict['MAP_IDS'] = ['']
          df_dict['REDUCE_IDS'] = ['']
        
        df_dict = pd.DataFrame.from_dict(df_dict)
        df = df.append(df_dict)

    df = df.drop_duplicates()

    jobs_ts={}
    for i,row in df.iterrows():
      # print(row)
      if row['NEW_JOB_ID'] not in list(jobs_ts.keys()):
        # if row['NEW_JOB_ID']:
          jobs_ts[row['NEW_JOB_ID']] = row['TimeStamp']
    jobs_ts  = simplejson.loads(simplejson.dumps(jobs_ts, ignore_nan=True))
    print("TIME STAMP FOR JOBS:",jobs_ts)
    print("-"*30)
    del jobs_ts['null']
    del jobs_ts['']
    jobs_ts


    jobs_dict={}
    for i,row in df.iterrows():
      if row['NEW_JOB_ID'] not in list(jobs_dict.keys()):
        # if row['NEW_JOB_ID']:
          jobs_dict[row['NEW_JOB_ID']] = str(row['MAP_IDS']).split(',')+str(row['REDUCE_IDS']).split(',')
          
    print("JOB_IDS:TASKS",jobs_ts)
    print("-"*30)

    jobs_dict  = simplejson.loads(simplejson.dumps(jobs_dict, ignore_nan=True))
    del jobs_dict['null']
    del jobs_dict['']


    ts_end={}
    ts_start={}
    for v in jobs_dict.values():
      for i in v:
        for c,row in df.iterrows():
          if row['TASK_DONE_ID']==i:
            ts_end[i]= datetime.strptime(row['TimeStamp'], "%Y-%m-%dT%H:%M:%S.%f")
          if row['RUN_TASK_ID']==i:
            ts_start[i]=datetime.strptime(row['TimeStamp'], "%Y-%m-%dT%H:%M:%S.%f")

    jobs_end={}
    for k,v in jobs_ts.items():
      jobs_end[k]=max(ts_end[i] for i in jobs_dict[k])
    print(jobs_end)
    print("-"*30)

    time_taken_job={}
    for k in jobs_ts:
      time_taken_job[k] = abs(jobs_end[k] - datetime.strptime(jobs_ts[k], "%Y-%m-%dT%H:%M:%S.%f")).total_seconds()
    time_taken_job
    print(time_taken_job)
    print("-"*30)

    time_taken_task={}
    for k in ts_end:
      time_taken_task[k] = abs(ts_start[k]-ts_end[k]).total_seconds()
    time_taken_task

    df_tasks = df_tasks.append(pd.DataFrame.from_dict({'FILENAME':[fileName],'MEAN':[statistics.mean(time_taken_task.values())], 'MEDIAN':[statistics.median(time_taken_task.values())]}))
    df_jobs = df_jobs.append(pd.DataFrame.from_dict({'FILENAME':[fileName],'MEAN':[statistics.mean(time_taken_job.values())], 'MEDIAN':[statistics.median(time_taken_job.values())]}))

fig_tasks = df_tasks.set_index('FILENAME').plot(rot=15,kind='bar',title='Plot of Tasks').get_figure()
fig_tasks.savefig('tasks.png')
fig_jobs = df_jobs.set_index('FILENAME').plot(rot=15,kind='bar',title='Plot of Jobs').get_figure()
fig_jobs.savefig('jobs.png')
