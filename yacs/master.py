"""Master node
This program runs on the master.
"""
from threading import Thread
from queue import Queue
from enum import Enum
import socket
import json
import random
import time

class Status(Enum):
    SUCCESS = 1
    PENDING = 2
    FAILED = 3


'''class Scheduler:

    def __init__(scheduler):
        self.scheduler = scheduler

    @staticmethod
    def random_scheduler(tasks, workers):
        total_free_slots=0
        num=0
        total=len(tasks)#number of tasks
        for w in workers:
            num+=1
            total_free_slots+=w['free_slot_count']
        
        while(total):#until task list in empty
            for task in tasks:
                if task['depends'] == []:#if is a task with no dependencies
                    while(total_free_slots==0):#check for free slots
                        time.sleep(1)
                        num=0
                        for w in workers:#check for number of slots
                            num+=1
                            total_free_slots+=w['free_slot_count']

                    if(total_free_slots > 0):
                        x = random.randrange(0,num)
                        if workers[x]['slots'] > 0:
                            task['wid'] = workers[x]['worker_id']
                            workers[x]['free_slot_count']-=1#updating free slot of that worker
                            master.send_task(task)
                            #total_free_slots -= 1
                            total-=1
        

    @staticmethod
    def round_robin_scheduler(tasks, workers):
        total_free_slots=0
        num=0
        total=len(tasks)#number of unassigned tasks
        
        for w in workers:
            num+=1
            total_free_slots+=w['free_slot_count']
        #tj1m1,j1m2,j1m3,j1r1,j2m1,j2m2
        c = 0
        #mytasks=task[:]
        while(total):#until task list in empty
            for task in tasks:
                if task['depends'] == []:#if is a task with no dependencies
                    while(total_free_slots==0):#check for free slots
                        time.sleep(1)
                        num=0
                        for w in workers:#check for number of slots
                            num+=1
                            total_free_slots+=w['free_slot_count']

                    if total_free_slots > 0 :
                        if workers[c]['slots'] > 0:
                            task['wid'] = workers[c]['worker_id']
                            workers[c]['free_slot_count']-=1
                                #total_free_slots -= 1
                            master.send_task(task)
                            c = (c+1)%num
                            total-=1
                else:
                    continue
                #mytasks.remove(task)
        
    @staticmethod
    def least_loaded_scheduler(tasks, workers):
        total_free_slots=0
        total=len(tasks)
        num=0
        for w in workers:
            num+=1
            total_free_slots+=w['free_slot_count']
        #task_mapping = []
            
        while(total):#until task list in empty
            for task in tasks:
                if task['depends'] == []:#if is a task with no dependencies
                    while(total_free_slots==0):#check for free slots
                        time.sleep(1)
                        num=0
                        for w in workers:#check for number of slots
                            num+=1
                            total_free_slots+=w['free_slot_count']

                    if(total_free_slots > 0):
                        if task['depends'] == []:
                            y=list(map(lambda x: x['free_slot_count'], workers))
                            x=y.index(max(y))
                            if workers[x]['slots'] > 0:
                                task['wid'] = workers[x]['worker_id']
                                workers[x]['free_slot_count']-=1
                                master.send_task(task)
                                total_free_slots -= 1'''

       
class Master:
    def __init__(self, config_path, scheduler):
        with open(config_path, 'r') as cfg_file:
            config = json.load(cfg_file)
        # for now I will not use a separate worker class, as this is simpler
        self.workers = config['Workers']
        for w in self.workers:
            w['free_slot_count'] = w['slots']
        
        #self.scheduler=Scheduler(sc)
        self.scheduler = scheduler
        self.threads = []
        self.connections = []
        #self.job_pool = list()
        self.task_pool = list()
        # threads and connections
        self.request_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.worker_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    # For context manager, establishes the connections and launches them in a new thread
    def __enter__(self):
        # conn_info is just a tuple of (conn, address) that comes from sock.accept()
        #self.request_conn_info = self.establish_connection(self.request_sock,1)
        #self.worker_conn_info = self.establish_connection(self.worker_sock,0)

        #self.connections.extend([self.request_conn_info, self.worker_conn_info])
        self.request_thread = Thread(target=self.request_listener, \
                                     args=(self.request_sock,))
        self.request_thread.start()

        self.worker_thread = Thread(target=self.worker_listener, \
                                     args=(self.worker_sock,))
        self.worker_thread.start()

        #self.threads.extend([request_listener_thread, worker_listener_thread])
        return self

    # cleanup of resources
    def __exit__(self, exc_type, exc_value, traceback):
        self.request_sock.close()
        self.worker_sock.close()
        for conn, _ in self.connections:
            conn.close()
        for thread in self.threads:
            thread.join()

    '''def establish_connection(self,s,flag):
        if(flag):
            port=5000#requests
        else:
            port=5001#workers
            
        s.bind(('',port))
        s.listen(5)
        return s.accept()'''

    
    def random_scheduler(self):
        tasks=self.task_pool  
        workers=self.workers
        total_free_slots=0
        num=0
        total=len(tasks)#number of tasks
        for w in workers:
            num+=1
            total_free_slots+=w['free_slot_count']
        
        while(total):#until task list in empty
            for task in tasks:
                if task['depends'] == []:#if is a task with no dependencies
                    while(total_free_slots==0):#check for free slots
                        time.sleep(1)
                        num=0
                        for w in workers:#check for number of slots
                            num+=1
                            total_free_slots+=w['free_slot_count']

                    if(total_free_slots > 0):
                        x = random.randrange(0,num)
                        if workers[x]['slots'] > 0:
                            task['wid'] = workers[x]['worker_id']
                            workers[x]['free_slot_count']-=1#updating free slot of that worker
                            master.send_task(task)
                            #total_free_slots -= 1
                            total-=1
        

    def round_robin_scheduler(self):
        tasks=self.task_pool  
        workers=self.workers
        total_free_slots=0
        num=0
        total=len(tasks)#number of unassigned tasks
        
        for w in workers:
            num+=1
            total_free_slots+=w['free_slot_count']
        #tj1m1,j1m2,j1m3,j1r1,j2m1,j2m2
        c = 0
        #mytasks=task[:]
        while(total):#until task list in empty
            for task in tasks:
                if task['depends'] == []:#if is a task with no dependencies
                    while(total_free_slots==0):#check for free slots
                        time.sleep(1)
                        num=0
                        for w in workers:#check for number of slots
                            num+=1
                            total_free_slots+=w['free_slot_count']

                    if total_free_slots > 0 :
                        if workers[c]['slots'] > 0:
                            task['wid'] = workers[c]['worker_id']
                            workers[c]['free_slot_count']-=1
                                #total_free_slots -= 1
                            master.send_task(task)
                            c = (c+1)%num
                            total-=1
                else:
                    continue
                #mytasks.remove(task)
        
    def least_loaded_scheduler(self):
        tasks=self.task_pool  
        workers=self.workers
        total_free_slots=0
        total=len(tasks)
        num=0
        for w in workers:
            num+=1
            total_free_slots+=w['free_slot_count']
        #task_mapping = []
            
        while(total):#until task list in empty
            for task in tasks:
                if task['depends'] == []:#if is a task with no dependencies
                    while(total_free_slots==0):#check for free slots
                        time.sleep(1)
                        num=0
                        for w in workers:#check for number of slots
                            num+=1
                            total_free_slots+=w['free_slot_count']

                    if(total_free_slots > 0):
                        if task['depends'] == []:
                            y=list(map(lambda x: x['free_slot_count'], workers))
                            x=y.index(max(y))
                            if workers[x]['slots'] > 0:
                                task['wid'] = workers[x]['worker_id']
                                workers[x]['free_slot_count']-=1
                                master.send_task(task)
                                total_free_slots -= 1


    def request_listener(self):

        port= 5000
        self.request_sock.bind('localhost',port)
        self.request_sock.listen(5)
        while(1):
            connection,addr=self.request_sock.accept()

            while(1):
                message=connection.recv(1024)
                data=message.decode("utf-8")
                '''data = {'job_id': '0',
                'map_tasks': [{'task_id': '0_M0', 'duration': 3}, {'task_id': '0_M1', 'duration': 3}, {'task_id': '0_M2', 'duration': 2}],
                'reduce_tasks': [{'task_id': '0_R0', 'duration': 3}, {'task_id': '0_R1', 'duration': 3}]}'''
                req = json.load(data)
                job = {}
                job['id'] = req['job_id']
                job['tasks'] = list()

                maptasks=list()#stores maptask ids
                redtasks=list()#stores redtask ids

                for red in data['reduce_tasks']:
                     maptasks.append(red['task_id'])

                for mapper in data['map_tasks']:
                    task={}
                    task['job_id']:job['id']
                    task['task_id']:mapper['task_id']
                    maptasks.append(mapper['task_id'])
                    task['duration']:mapper['duration']
                    task['wid']:None
                    task['depends']:None
                    task['satisfies']:redtasks
                    task['status']:Status(2)
                    self.task_pool.append(task)
                    job['tasks'].append(task)

                
                for red in data['reduce_tasks']:
                    task={}
                    task['job_id']:job['id']
                    task['task_id']:red['task_id']
                    task['duration']:red['duration']
                    task['wid']:None
                    task['depends']:maptasks
                    task['satisfies']:None
                    task['status']:Status(2)
                    self.task_pool.append(task)
                    job['tasks'].append(task)

                #self.job_pool.put(job)#adding job to jobpool
        

    def worker_listener(self):
        s=self.worker_sock
        
        port= 5001
        s.bind('localhost',port)
        s.listen(5)
        while(1):
            connection,addr=s.accept()

            while(1):
                message=connection.recv(1024)#dict task
                data=message.decode("utf-8")
                task=json.loads(data)
                #need to update task pool
                #need to update satisfies if it is map task
                self.update_worker_params(task)


    def update_worker_params(self,task):
        key=self.task_pool.index(task)
        self.task_pool[key]['status']=Status(1)
     
        for i in self.task_pool:
            if task['task_id'] in i['depends']:
                i['depends'].remove(task['task_id'])

        wid=task['wid']
        for w in self.workers:
            if(w['worker_id']==wid):
                w['free_slot_count']+=1
        

        #code for updation
    
    def run(self):
        while(1):
            if(len(self.task_pool)==0):
                time.sleep(1)
            
        if(self.scheduler=='round robin'):
            self.round_robin_scheduler()

        if(self.scheduler=='random'):
            self.random_scheduler()

        if(self.scheduler=='least loaded'):
            self.least_loaded_scheduler()

        

        


    #def start_job(self):
        #pass

    def send_task(self, task):
        wid=task['wid']
        port=0#initializing port
        for worker in self.workers:
            if(worker['worker_id']==wid):
                port=worker['port']
                break
        #need to find which port number to send to
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("localhost", port))#sending message to worker 
            message=json.dumps(task)
            #send task
            s.send(message.encode())
            s.close()

if __name__ == '__main__':
    # Context management protocol, all threads and connections
    # are automatically closed after it's done

    with Master("path/to/config","round_robin_sheduler") as master:
        master.run()
        
