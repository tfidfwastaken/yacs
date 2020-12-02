'''
job_log.csv fmt :
jid     start       end

worker_<wid>.csv fmt :
jid     tid     start   end
'''
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, date
from statistics import mean, median
import sys
import json

def get_pts(w):
    #start = [1,3,4,7]
    start = w[2]
    #end = [2,5,6,8]
    end = w[3]
    st = []
    en = []
    for i in range(len(start)):
        st.append((start[i],0))
    for i in range(len(end)):
        en.append((end[i],1))
    combined = st + en
    combined.sort()
    c = 0
    twc = []
    res = []
    for i in combined:
        if i[1] == 0:
            c+=1
        else:
            c-=1
        twc.append((i[0],c))
    base = combined[0][0]
    for i in twc:
        x = i[0] - base
        res.append((x,i[1]))
    return res

if __name__ == '__main__':
    # Scheduler
    sched = input("Scheduler : ")

    # Job log file
    job_log_csv = input("Log file for job : ")
    job_log = pd.read_csv(job_log_csv, header = None)

    # Mean and median of job completion times
    job_comp_times = job_log[2] - job_log[1]
    print("Total number of jobs : ",len(job_log))
    print("Mean job completion time = ",job_comp_times.mean())
    print("Median job completion time = ",job_comp_times.median())

    # Using config file to figure out the wids
    cfg = open('config.json')
    workers = json.load(cfg)['workers']
    wid_list = []
    for w in workers:
        wid_list.append(w['worker_id'])
    print("Worker IDs : ", wid_list)

    # Number of workers
    num_workers = len(wid_list)

    # Getting worker log files
    worker_logs = dict()
    for i in wid_list:
        #fname = input("Log file for "+str(i)+" : ")
        fname = "worker_"+str(i)+".csv"
        worker_logs[i] = pd.read_csv(fname, header = None)

    # Initializing to record the task completion times
    overall_task_comp = []
    # Initializing to record number of tasks per worker
    num_tasks_per_worker = dict()

    # Plotting number of tasks per worker against time
    for w in worker_logs:
        wdf = worker_logs[w]
        print("Worker ",str(w)," :")
        print("Number of tasks :",len(wdf))

        # Updating number of workers per
        num_tasks_per_worker[w] = len(wdf)

        # Task completion times for this worker
        task_comp_times = wdf[3] - wdf[2]

        # Updating task completion times to calculate mean and median
        overall_task_comp.extend(task_comp_times)

        # Getting the points for the graph
        w_pts = get_pts(wdf)
        x1 = [i[0] for i in w_pts]
        y1 = [i[1] for i in w_pts]
        # Plotting graph
        plt.figure("Worker "+str(w)+" : "+sched)
        plt.step(x1,y1)
        plt.show()

    # Converting the overall_task_comp into dataframe to do mean and median
    print(overall_task_comp)
    task_comp = pd.DataFrame([overall_task_comp])
    print("Mean task completion time = ",task_comp.mean()[1])
    print("Median task completion time = ",task_comp.median()[1])

    tasks_per_worker = num_tasks_per_worker.values()
    # Plotting graph for number of tasks per worker
    plt.figure("Tasks per Worker : "+sched)
    plt.bar(wid_list, tasks_per_worker, label = sched, color = "#87ceeb" , width = 0.3)
    plt.legend()
    plt.title("Number of tasks pe worker")
    plt.xlabel("Workers")
    plt.ylabel("Number of tasks")
    plt.show()
