"""
Master Node
This program runs on the master.
"""
from threading import Thread
from queue import Queue
from multiprocessing import SimpleQueue
from enum import Enum
import socket
import json
import random
import time
import sys
from threading import *

def Status(x):
    status = {
        1 : "SUCCESS",
        2 : "PENDING",
        3 : "FAILED",
        4 : "ONGOING"
    }
    return status[x]

def show_workers(workers):
    ###
    print("--------------------------------")
    print("WorkerID\tFree slots")
    print("--------------------------------")
    for w in workers:
        print(w['worker_id'],"\t\t",w['free_slot_count'])
    print("--------------------------------")

def show_task_pool(tp):
    print("------------------------------------------------------")
    print("TaskID\tWID\tStatus\t\tDepencies")
    print("------------------------------------------------------")
    for task in tp:
        print(task,"\t",tp[task]['wid'],"\t",tp[task]['status'],"\t",tp[task]['depends'])
    print("------------------------------------------")

class Master:
    # Constructor
    def __init__(self, config_path, scheduler):
        print("Constructing...")
        with open(config_path, 'r') as cfg_file:
            config = json.load(cfg_file)
        # for now I will not use a separate worker class, as this is simpler
        self.workers = config['workers']
        for w in self.workers:
            w['free_slot_count'] = w['slots']
        self.scheduler = scheduler
        self.threads = []
        self.connections = []
        self.job_pool = SimpleQueue()
        self.task_pool = dict()
        self.job_tracker = dict()
        self.tp_lock = Semaphore(1)
        self.w_lock = Semaphore(1)
        # threads and connections
        # self.request_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.worker_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("INITIAL VALUES:")
        print("Workers :\n", self.workers)

    # For context manager, establishes the connections and launches them in a new thread
    def __enter__(self):
        self.request_thread = Thread(target=self.request_listener)
        self.request_thread.start()

        self.worker_thread = Thread(target=self.worker_listener)
        self.worker_thread.start()
        return self

    # cleanup of resources
    def __exit__(self, exc_type, exc_value, traceback):
        #self.request_sock.close()
        print("In exit...")
        #self.worker_sock.close()
        for conn, _ in self.connections:
            conn.close()
        for thread in self.threads:
            thread.join()


    def request_listener(self):
        # Master port number
        port = 5000
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('localhost',port))
        s.listen(5)
        while (1):
            connection, addr = s.accept()
            message=connection.recv(1024)
            '''
            data = {'job_id': '0',
            'map_tasks': [{'task_id': '0_M0', 'duration': 3}, {'task_id': '0_M1', 'duration': 3}, {'task_id': '0_M2', 'duration': 2}],
            'reduce_tasks': [{'task_id': '0_R0', 'duration': 3}, {'task_id': '0_R1', 'duration': 3}]}
            '''
            if(len(message)!=0):
                print("Job Request Received...\n")
                data = json.loads(message)
                '''
                Job_pool -> Queue of jobs
                Job -> list of tasks
                Task -> dict with <k,v> as <taskid, details>
                job = [ {m0 : {deets}}, {m1 : {deets}}, {r0 : {deets}}, {task4} ]
                '''
                # Initializing job
                job = list()
                # Getting job id
                j_id = data['job_id']
                '''
                # Getting map tasks
                m_tasks -> list of map tasks
                map task -> dict()
                looks like this -
                [{'task_id': '0_M0', 'duration': 3}, {'task_id': '0_M1', 'duration': 3}, {'task_id': '0_M2', 'duration': 2}]
                '''
                m_tasks = data['map_tasks']
                # Getting list of red tasks (similar to above)
                r_tasks = data['reduce_tasks']
                mtaskids = []
                rtaskids = []

                for r in r_tasks:
                     rtaskids.append(r['task_id'])

                for mapper in m_tasks:
                    m_id = mapper['task_id']
                    print("Noting map task :", m_id)
                    mtaskids.append(m_id)
                    task={}
                    task['job_id'] = j_id
                    task['task_id'] = mapper['task_id']
                    task['duration'] = mapper['duration']
                    task['wid'] = []
                    task['depends'] = []
                    task['satisfies'] = rtaskids
                    task['status'] = Status(2)
                    t = dict()
                    t[m_id] = task
                    job.append(t)

                for reducer in r_tasks:
                    r_id = reducer['task_id']
                    print("Noting reducer task :", r_id)
                    task={}
                    task['job_id']=j_id
                    task['task_id']=reducer['task_id']
                    task['duration']=reducer['duration']
                    task['wid'] = []
                    task['depends']=mtaskids
                    task['satisfies'] = []
                    task['status']=Status(2)
                    t = dict()
                    t[r_id] = task
                    job.append(t)
                # put the job in the queue
                self.job_pool.put(job)
                self.job_tracker[j_id] = Status(2)

    def run(self):
        print("In master.run()...")
        while(1):
            if self.job_pool.empty()==False:
                print("There is a job...")
                job = master.job_pool.get()
                # job is a list of tasks which are rep as dict()
                # job = [ {m0 : {deets}}, {m1 : {deets}}, {r0 : {deets}}, {task4} ]
                # task pool = {m0 :{deets}, m1: {deets}}
                print("Constructing task pool...")

                ### Acquire task pool lock
                self.tp_lock.acquire()

                for j in job:
                    for task in j:
                        self.task_pool[task] = j[task]
                        j_id = j[task]['job_id']
                print("Task pool constructed :")
                self.show_task_pool(self.task_pool)
                print("Number of tasks = ", len(self.task_pool))

                ### Release task pool lock
                self.tp_lock.release()

                self.start_job(j_id)
                if self.job_tracker[j_id] != "SUCCESS":
                    time.sleep(1)
                self.show_job_tracker()


    def find_task(self):
        ### Acquire task pool lock
        self.tp_lock.acquire()

        id = -1
        for task in self.task_pool:
            if self.task_pool[task]['depends'] == [] and self.task_pool[task]['status'] == "PENDING" :
                id = self.task_pool[task]['task_id']
        ### Release task pool lock
        self.tp_lock.release()

        return id

    def start_job(self,j_id):
        total = len(self.task_pool)
        while(total>0):
            tfs = self.find_free()
            if (tfs>0):
                tid = self.find_task()
                if tid != -1:
                    print("\nAssigning Task : ",tid)
                    # Worker ID
                    winfo = self.random_sched(1)
                    print("WINFO = ",winfo[0])
                    # Update the status
                    self.task_pool[tid]['status'] = Status(4)
                    # Give it a wid
                    self.task_pool[tid]['wid'] = winfo[0]
                    self.show_task_pool(self.task_pool)
                    # Send the task
                    self.send_task(self.task_pool[tid])
                    print("WINFO = ",winfo)
                    #print("wc = ",wc)
                    self.update_worker_params(winfo[0],-1)
                    total -= 1
                    print("total = ",total)
        print("OUTTA WHILE SO TOTAL = ",total)
        while (len(self.task_pool) != 0):
            time.sleep(1)
        if (len(self.task_pool)==0):
            print("MR JOB DONE!!!!!!!!!")
            self.job_tracker[j_id] = Status(1)

    def random_sched(self,x):
        winfo = []
        while (x>0):
            r = random.randrange(0,len(self.workers))
            if self.workers[r]['free_slot_count'] > 0:
                w = self.workers[r]['worker_id']
                winfo.append(w)
                x -= 1
        # print("WINFO :",winfo)
        return winfo

    def send_task(self, task):
        print("SENDING :\n",task)
        wid=task['wid']
        port=[worker['port'] for worker in self.workers if worker['worker_id']==wid ]
        print("Worker : ",wid)
        print("Port : ", port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("localhost", port[0]))#sending message to worker
            message=json.dumps(task)
            #send task
            s.send(message.encode())
            s.close()

    def worker_listener(self):
        print("Listening to workers now...\n\n")
        s =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        port = 5001
        s.bind(('localhost',port))
        s.listen(5)
        new_conn = []
        while(1):
            connection,addr=s.accept()
            if addr not in new_conn:
                new_conn.append(addr)
                th = Thread(target=self.worker_update, args=(connection,addr,))
                th.start()

    def worker_update(self,connection,addr):
        while(1):
            message=connection.recv(1024)
            if(len(message)!=0):
                self.proc_worker_update(message)

    def proc_worker_update(self,message):
        data=message.decode()
        print(data)
        print()
        task=json.loads(data)
        wid = task['wid']
        t_id = task['task_id']
        print("\nProcessing update from WORKER ",wid)
        self.update_task_pool_params(t_id)
        self.update_worker_params(wid,1)

    def update_task_pool_params(self,t):
        ### Acquire task pool lock
        self.tp_lock.acquire()

        print("Updating taskpool...")
        print("Taskpool before updation :")
        self.show_task_pool(self.task_pool)
        ### Get taskpool lock
        red_modify = []
        if "M" in t:
            red_modify = self.task_pool[t]['satisfies']
        print("Reduce task which depend on this : ", red_modify)
        # self.task_pool[rm]['depends'].remove(t)

        for rm in red_modify:
            print(self.task_pool[rm]['depends'])
            if t in self.task_pool[rm]['depends'] :
                self.task_pool[rm]['depends'].remove(t)
            self.show_task_pool(self.task_pool)

        # remove the task from task pool
        del self.task_pool[t]
        print("Taskpool after updation :")
        self.show_task_pool(self.task_pool)

        ### Release task pool lock
        self.tp_lock.release()

    def update_worker_params(self,wid,x):
        ### Acquire worker lock
        self.w_lock.acquire()

        print("Updating worker params for worker ",wid)
        for w in self.workers:
            if w['worker_id'] == int(wid):
                print("In here...")
                w['free_slot_count'] += x

        print("Workers after update :")
        self.show_workers(self.workers)

        ### Release worker lock
        self.w_lock.release()

    def find_free(self):
        ### Acquire worker lock
        self.w_lock.acquire()

        t = 0
        for w in self.workers:
            t += w['free_slot_count']

        ### Release worker lock
        self.w_lock.release()
        return t

    def show_workers(self,workers):
        print("--------------------------------")
        print("WorkerID\tFree slots")
        print("--------------------------------")
        for w in workers:
            print(w['worker_id'],"\t\t",w['free_slot_count'])
        print("--------------------------------")

    def show_task_pool(self,tp):
        print("------------------------------------------------------")
        print("TaskID\tWID\tStatus\t\tDepencies")
        print("------------------------------------------------------")
        for task in tp:
            print(task,"\t",tp[task]['wid'],"\t",tp[task]['status'],"\t",tp[task]['depends'])
        print("------------------------------------------------------")

    def show_job_tracker(self):
        print("-----------------")
        print("Job\tStatus")
        print("-----------------")
        for job in self.job_tracker:
            print(job,"\t",self.job_tracker[job])

if __name__ == '__main__':
    # Context management protocol, all threads and connections
    # are automatically closed after it's done

    '''config=sys.argv[1]
    scheduler=sys.argv[2]'''
    with Master("config.json","random") as master:
        master.run()





'''
def establish_connection(self,s,flag):
    if(flag):
        port=5000#requests
    else:
        port=5001#workers

    s.bind(('',port))
    s.listen(5)
    return s.accept()
'''
'''
def __enter__(self):
    # conn_info is just a tuple of (conn, address) that comes from sock.accept()
    self.request_conn_info = self.establish_connection(self.request_sock,1)
    self.worker_conn_info = self.establish_connection(self.worker_sock,0)
    self.connections.extend([self.request_conn_info, self.worker_conn_info])
    self.request_thread = Thread(target=self.request_listener, \
                                 args=(self.request_sock,))
    self.request_thread.start()

    self.worker_thread = Thread(target=self.worker_listener, \
                                 args=(self.worker_sock,))
    self.worker_thread.start()

    #self.threads.extend([request_listener_thread, worker_listener_thread])
    return self
'''
'''
    def start_job(self):
        total = len(self.task_pool)
        while(total):
            tfs = self.find_free()
            if (tfs>0):
                #print("Free slots available : ",tfs)
                x = tfs if (total>tfs) else total
                #print("x = ",x)
                #print("total = ", total)
                winfo = self.random_sched(x)
                wc = 0
                # print("WINFO : ",winfo)
                # show_task_pool(self.task_pool)
                for task in self.task_pool:
                    if x>0 :
                        #print("X = ",x)
                        if self.task_pool[task]['depends'] == [] and self.task_pool[task]['status'] == "PENDING" :
                            print("\nAssigning...")
                            # Update the status
                            self.task_pool[task]['status'] = Status(4)
                            # Give it a wid
                            self.task_pool[task]['wid'] = winfo[wc]
                            show_task_pool(self.task_pool)
                            wc += 1
                            # Send the task
                            self.send_task(self.task_pool[task])
                            self.update_worker_params(winfo[wc],-1)
                            #print("here")

                            #Need to put something to handle a send fail
                            #while(st == "FAILED"):
                        #        st = send_task(tp[task])
                        #    print("Task sent successfully")

                            x -= 1
                            total -= 1
'''
'''
def start_job(self):
    total = len(self.task_pool)
    while(total>0):
        tfs = self.find_free()
        if (tfs>0):
            x = tfs if (total>tfs) else total
            winfo = self.random_sched(x)
            wc = 0
            while (x>0):
                tid = self.find_task()
                if tid != -1:
                    print("\nAssigning Task : ",tid)
                    # Update the status
                    self.task_pool[tid]['status'] = Status(4)
                    # Give it a wid
                    self.task_pool[tid]['wid'] = winfo[wc]
                    show_task_pool(self.task_pool)
                    # Send the task
                    self.send_task(self.task_pool[tid])
                    print("WINFO = ",winfo)
                    print("wc = ",wc)
                    self.update_worker_params(winfo[wc],-1)
                    wc += 1
                    x -= 1
                    print("x = ",x)
                    total -= 1
                    print("total = ",total)
    print("OUTTA WHILE SO TOTAL = ",total)
    while (len(self.task_pool) != 0):
        time.sleep(1)
    if (len(self.task_pool)==0):
        print("MR JOB DONE!!!!!!!!!")
'''
