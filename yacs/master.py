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


class Scheduler:
    @staticmethod
    def random_scheduler(tasks, workers):
        total_free_slots=0
        num=0
        for w in workers:
            num+=1
            total_free_slots+=w['free_slot_count']
        #task_mapping = []
            
        for task in tasks:
            while(total_free_slots==0):
                time.sleep(1)

            if(total_free_slots > 0):
                if task['depends'] == []:
                    x = random.randrange(0,num)
                    if workers[x]['slots'] > 0:
                        task['wid'] = workers[x]['worker_id']
                        workers[x]['free_slot_count']-=1
                        master.send_task(task)
                        total_free_slots -= 1
        

    @staticmethod
    def round_robin_scheduler(tasks, workers):
        total_free_slots=0
        num=0
        for w in workers:
            num+=1
            total_free_slots+=w['free_slot_count']
        #task_mapping = []
        c = 0
        
        for task in tasks:
            while(total_free_slots==0):
                time.sleep(1)

            if total_free_slots > 0 :
                if task.depends_on == []:
                        if workers[c]['slots'] > 0:
                            task['wid'] = workers[c]['worker_id']
                            workers[c]['free_slot_count']-=1
                            total_free_slots -= 1
                            master.send_task(task)
                        c = (c+1)%num
        
    @staticmethod
    def least_loaded_scheduler(tasks, workers):
        total_free_slots=0
        num=0
        for w in workers:
            num+=1
            total_free_slots+=w['free_slot_count']
        #task_mapping = []
            
        for task in tasks:
            while(total_free_slots==0):
                time.sleep(1)

            if(total_free_slots > 0):
                if task['depends'] == []:
                    y=list(map(lambda x: x['free_slot_count'], workers))
                    x=y.index(max(y))
                    if workers[x]['slots'] > 0:
                        task['wid'] = workers[x]['worker_id']
                        workers[x]['free_slot_count']-=1
                        master.send_task(task)
                        total_free_slots -= 1
       
class Master:
    def __init__(self, config_path, scheduler):
        with open(config_path, 'r') as cfg_file:
            config = json.load(cfg_file)
        # for now I will not use a separate worker class, as this is simpler
        self.workers = config['Workers']
        for w in self.workers:
            w['free_slot_count'] = w['slots']
        self.scheduler = scheduler
        self.threads = []
        self.connections = []
        self.job_pool = Queue()
        self.task_pool = Queue()
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

    def request_listener(self, s):

        port= 5000
        s.bind('localhost',port)
        s.listen(5)
        while(1):
            connection,addr=s.accept()

            while(1):
                message=connection.recv(1024)
                data=message.decode("utf-8")
                '''data = {'job_id': '0',
                'map_tasks': [{'task_id': '0_M0', 'duration': 3}, {'task_id': '0_M1', 'duration': 3}, {'task_id': '0_M2', 'duration': 2}],
                'reduce_tasks': [{'task_id': '0_R0', 'duration': 3}, {'task_id': '0_R1', 'duration': 3}]}'''
                req = json.load(data)
                job = {}
                job['id'] = req['job_id']
                job['tasks'] = []

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
                    self.task_pool.put(task)
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
                    job['tasks'].append(task)

                self.job_pool.put(job)#adding job to jobpool
        

    def worker_listener(self, s):
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
        wid=task['wid']
        for w in self.workers:
            if(w['worker_id']==wid):
                w['free_slot_count']+=1
        

        #code for updation
    
    def run(self):
        pass

    def start_job(self):
        pass

    def send_task(self, task):
        #need to find which port number to send to
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("localhost", port))#sending message to worker 
            message=json.dumps(task)
            #send task
            s.send(message.encode())

if __name__ == '__main__':
    # Context management protocol, all threads and connections
    # are automatically closed after it's done
    with Master("path/to/config","round_robin_sheduler") as master:
        master.run()
