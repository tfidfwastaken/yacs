
"""Master node

This program runs on the master.
"""
from threading import Thread
from queue import SimpleQueue
from enum import Enum
import socket
import json
import sys

class Status(Enum):
    SUCCESS = 1
    PENDING = 2
    FAILED = 3


class Scheduler:
    @staticmethod
    def random_scheduler(tasks, workers):
        task_mapping = []
        while total_free_slots > 0 :
            for task in tasks:
                if task.depends_on == []:
                    tm = {}
                    tm['task'] = task.task_id
                    tm['w_id'] = 0
                    x = random.randrange(1,5)
                    while tm['w_id'] == 0:
                        if workers[x]['slots'] > 0:
                            tm['w_id'] = worker[x]['worker_id']
                            tm['task'] = task.task_id
                            task_mapping.append(tm)
                            total_free_slots -= 1
                        x = random.randrange(1,5)
        return task_mapping

    @staticmethod
    def round_robin_scheduler(tasks, workers):
        # Assuming len(self.workers) will give me total number of workers
        task_mapping = []
        c = 0
        while total_free_slots > 0 :
            for task in tasks:
                if task.depends_on == []:
                    tm = {}
                    tm['task'] = task.task_id
                    tm['w_id'] = 0
                    while tm['w_id'] == 0:
                        if workers[c]['slots'] > 0:
                            tm['w_id'] = worker[c]['worker_id']
                            total_free_slots -= 1
                        c = (c+1)%len(self.workers)
        return task_mapping

    @staticmethod
    def least_loaded_scheduler(tasks, workers):
        pass

class Task:
    def __init__(self, task_id, job_id, dur, dep, sat):
        self.task_id = task_id
        self.job_id = job_id
        self.duration = dur
        self.depends_on = []
        self.satisfies = []
        self.status = 0 # Not done
        self.slot = 0 # No slot given

class Master:
    def __init__(self, config_path, scheduler):
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
        self.task_pool = SimpleQueue()

        # threads and connections
        self.request_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.worker_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # For context manager, establishes the connections and launches them in a new thread
    def __enter__(self):
        # conn_info is just a tuple of (conn, address) that comes from sock.accept()
        request_conn_info = self.establish_connection(self.request_sock)
        worker_conn_info = self.establish_connection(self.worker_sock)
        self.connections.extend([request_conn_info, worker_conn_info])
        self.request_thread = Thread(target=self.request_listener, \
                                     args=(self.request_conn_info,))
        self.request_thread.start()
        self.worker_thread = Thread(target=self.worker_listener, \
                                     args=(self.worker_conn_info,))
        self.worker_thread.start()
        self.threads.extend([request_listener_thread, worker_listener_thread])
        return self

    # cleanup of resources
    def __exit__(self, exc_type, exc_value, traceback):
        self.request_sock.close()
        self.worker_sock.close()
        for conn, _ in self.connections:
            conn.close()
        for thread in self.threads:
            thread.join()

    def request_listener(self, conn):
        # listens to requests from requets.py
        # will modify self.job_pool -> put tasks as part of the job dict
        # json processing
        data = {'job_id': '0',
                'map_tasks': [{'task_id': '0_M0', 'duration': 3}, {'task_id': '0_M1', 'duration': 3}, {'task_id': '0_M2', 'duration': 2}],
                'reduce_tasks': [{'task_id': '0_R0', 'duration': 3}, {'task_id': '0_R1', 'duration': 3}]}
        req = json.load(data)
        job = {}
        job['id'] = req['job_id']
        job['tasks'] = []

        red_ids = []
        for red in data['reduce_tasks']:
            red_ids.append(red['task_id'])
        map_ids = []

        for m in data['map_tasks']:
            task = Task(m['task_id'], job['id'], m['duration'], [], red_ids)
            job['tasks'].append(task)
            map_ids.append(m['task_id'])

        for r in data['reduce_tasks']:
            task = Task(r['task_id'], job['id'], r['duration'], map_ids, [])
            job['tasks'].append(task)

        job['status'] = Status(2)
        self.job_pool.put(job)

    def worker_listener(self, conn):
        # Modifies the status for completed tasks in job['tasks'] and task_pool
        # use update_worker_params()
        pass

    def update_worker_params(self, worker):
        # Shouldn't this have something to indicate whether we need to +/- slots?
        pass

    def run(self):
        pass

    def start_job(self):
        pass

    def send_task(self, task_mapping, conn):
        # use update_worker_params() to do worker['slots'] -= 1
        pass

if __name__ == '__main__':
    # Context management protocol, all threads and connections
    # are automatically closed after it's done
    if(len(sys.argv)!=3):
        print("Usage: python3 master.py <config file path> <scheduler>")
        exit()
    config_path = sys.argv[1]
    scheduler = sys.argv[2]
    with Master(config_path, scheduler) as master:
        master.run()

    while job.empty()==False:
        job = master.job_pool.get()
        master.start_job()

        # Put tasks for this job  into task pool here?
        for task in job['tasks']:
            master.task_pool.put(task)

        while job['status'] != 1:
            task_mapping = self.scheduler(task_pool, self.workers)
            send_task(task_mapping, conn)
            # How do I get connections?
