"""Master node

This program runs on the master.
"""

from threading import Thread, Event, Lock
from queue import SimpleQueue
import logging
import pprint
import sys
import random
from signal import signal, SIGINT
import socket
import json

from utils import TaskPool, Status


class PrettyLog():
    def __init__(self, obj):
        self.obj = obj
    def __repr__(self):
        return pprint.pformat(self.obj)


class Scheduler:
    @staticmethod
    def random_scheduler(tasks, workers):
        task_map_list = []
        for task in tasks:
            while True:
                worker = random.choice(workers)
                if worker['free_slot_count'] > 0:
                    task_map_list.append({
                        'worker_id': worker['worker_id'],
                        'task': task
                    })
                    break
        return task_map_list
                    
    def find_llw(w):
        max_wid = 0
        max_count = 0
        for wk in w :
            if w[wk] > max_count :
                max_wid = wk
                max_count = w[wk]
        return max_wid

    @staticmethod
    def least_loaded_scheduler(tasks, workers):
        w = dict()
        task_mapping = []
        for wk in workers:
            wid = wk['worker_id']
            w[wid] = wk['free_slot_count']

        for task in tasks:
            max_wid = self.find_llw(w)
            task_mapping.append({
                'worker_id': max_wid,
                'task': task
            })
            for worker in w:
                if worker == max_wid:
                    w[worker] -= 1
            print("TASK MAPPING", task_mapping)
            print("MODIFIED :\n",w)
        return task_mapping


    @staticmethod
    def least_loaded_scheduler(tasks, workers):
        wks = workers
        task_mapping = []
        # need to find number of free slots
        # find the least number
        min_wid = 0
        min_count = 0
        for task in tasks:
            for w in wks :
                if w['free_slot_count'] < min_count :
                    min_wid = w['worker_id']
                    min_count = w['free_slot_count']
                    w['free_slot_count'] -= 1
            task_mapping.append({
                'worker_id': w['worker_id'],
                'task': task
            })
        return task_mapping

    @staticmethod
    def round_robin_scheduler(tasks, workers):
        w = dict()
        task_mapping = []
        for wk in workers:
            wid = wk['worker_id']
            w[wid] = wk['free_slot_count']

        c = 0
        for task in tasks:
            flag = 1
            while flag:
                if w[c+1] > 0:
                    task_mapping.append({
                    'worker_id': c+1 ,
                    'task': task
                    })
                    w[c+1] -= 1
                    flag = 0
                c = (c+1)%3

        return task_mapping


class Master:
    def __init__(self, config_path, scheduler):
        with open(config_path, 'r') as cfg_file:
            config = json.load(cfg_file)
        # for now I will not use a separate worker class, as this is simpler
        self.workers = config['workers']
        for w in self.workers:
            w['free_slot_count'] = w['slots']
        self.scheduler = scheduler
        self.worker_threads = []
        self.connections = {}
        self.job_pool = SimpleQueue()
        self.task_pool = None

        # Connections
        self.request_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.worker_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Events and locks
        self.schedule_event = Event()
        self.scheduler_lock = Lock()
        self.worker_lock = Lock()

    # For context manager, establishes the connections and launches them in a new thread
    def __enter__(self):
        signal(SIGINT, self._sigint_handler)
        self.exit_command_received = Event()
        self.request_sock.bind(('localhost', 5000)) # fix later, hardcoded rn
        self.worker_sock.bind(('localhost', 5001)) # fix later, hardcoded rn

        logging.info("Starting request thread...")
        self.request_thread = Thread(name='Request Thread', \
                                     target=self.request_listener, \
                                     args=(self.request_sock,))
        self.request_thread.start()

        logging.info("Starting worker threads...")
        # listen for workers here and send them to a new thread
        self.worker_sock.listen()
        for worker in self.workers:
            logging.info(f"Awaiting worker {worker['worker_id']}...")
            conn_info = self.worker_sock.accept()
            logging.info(f"Connection with worker {worker['worker_id']} accepted!")
            self.connections[worker['worker_id']] = conn_info
            self.worker_thread = Thread(name=f"Worker Thread {worker['worker_id']}", \
                                        target=self.worker_listener, \
                                        args=(conn_info,))
            self.worker_thread.start()
        return self

    # cleanup of resources
    def __exit__(self, exc_type, exc_value, traceback):
        self.request_sock.shutdown(socket.SHUT_RD)
        self.request_sock.close()
        self.worker_sock.shutdown(socket.SHUT_RD)
        self.worker_sock.close()
        for conn, _ in self.connections.values():
            conn.shutdown(socket.SHUT_RD)
            conn.close()
        logging.debug("Closed connections sockets")
        for thread in self.worker_threads:
            thread.join()
        self.request_thread.join()
        logging.debug("Closed all threads")

    def _sigint_handler(self, signal_received, frame):
        print("Received exit command...")
        self.exit_command_received.set()
        self.__exit__(None, None, None)
        logging.info("Exiting...")
        print("Exiting...")
        sys.exit(0)

    def request_listener(self, sock):
        sock.listen()
        while not self.exit_command_received.is_set():
            logging.info("Awaiting request connections...")
            conn, addr = sock.accept()
            with conn:
                logging.info("Request source connected! Awaiting data...")
                req = conn.recv(4096).decode()
                logging.debug(f"request_listener:Got request:\n{PrettyLog(req)}")
                job = json.loads(req)
                self.job_pool.put(job)
                # logging.debug(f"Job Pool: {[job['job_id'] for job in list(job_pool.queue)]}")

    def worker_listener(self, conn_info):
        conn, addr = conn_info
        with conn:
            while not self.exit_command_received.is_set():
                logging.info(f"Listening for messages from {addr}")
                worker_message = conn.recv(4096).decode()
                if not worker_message:
                    logging.error(f"Connection from worker {addr} broken")
                    break
                logging.debug(f"Received from worker {addr}: {worker_message}")
                task_map = json.loads(worker_message)
                # increases the number of free slots for each worker in task map
                self.update_worker_params(task_map, mode='increment')
                # updates the dependencies of the task in the task pool, and
                # removes tasks from the task pool that are not needed anymore
                self.update_dependencies(task_map)
                # notify the scheduler that there are free slots
                self.schedule_event.set()

    def update_worker_params(self, task_map, mode):
        logging.debug("Updating worker information...")
        # O(n^2) and I don't care
        # Apologies to those reading this code snippet
        print(task_map)
        with self.worker_lock:
            for worker in self.workers:
                if worker['worker_id'] == task_map['worker_id']:
                    if mode == "increment":
                        worker['free_slot_count'] += 1
                    elif mode == "decrement":
                        worker['free_slot_count'] -= 1
        logging.debug(f"update_worker_params:Workers:\n{PrettyLog(self.workers)}")
                        
    # This part is a huge hack, proceed with caution
    def update_dependencies(self, task_map):
        task = task_map['task']
        logging.debug(f"Updating dependencies for task {task['task_id']}...")
        def update_func(t, id_to_remove):
            try:
                t['depends_on'].remove(id_to_remove)
            except ValueError:
                pass
        for task_id in task['satisfies']:
            logging.debug(f"Removing depends_on for {task_id}")
            self.task_pool.update(
                lambda t: t['task_id'] == task_id,
                lambda t: update_func(t, task['task_id'])
            )
        if task['status'] == 1:
            print(f"Deleting {PrettyLog(task)}")
            self.task_pool.remove(lambda t: t['task_id'] == task['task_id'])
        logging.debug(f"Updated task pool:\n{PrettyLog(self.task_pool.tasks)}")

    def run(self):
        while True:
            # Read from job pool
            logging.info("Waiting for Jobs...")
            job = self.job_pool.get()
            logging.info(f"Received from job pool: {job['job_id']}")
            self.start_job(job)

    def parse_job_request(self, job):
        map_tasks = job['map_tasks']
        reduce_tasks = job['reduce_tasks']
        for task in map_tasks:
            task['job_id'] = job['job_id']
            task['satisfies'] = [red_task['task_id'] for red_task in reduce_tasks]
            task['depends_on'] = []
            task['status'] = Status.UNSCHEDULED

        for task in reduce_tasks:
            task['job_id'] = job['job_id']
            task['satisfies'] = []
            task['depends_on'] = [map_task['task_id'] for map_task in map_tasks]
            task['status'] = Status.UNSCHEDULED

        tasks = map_tasks + reduce_tasks
        return tasks


    def start_job(self, job):
        logging.info(f"Starting job: {job['job_id']}")
        tasks = self.parse_job_request(job)
        self.task_pool = TaskPool(tasks)
        # print(self.task_pool.is_empty())

        total_free_slots = sum([w['free_slot_count'] for w in self.workers])
        # print(total_free_slots)
        # A schedulable task is one that has no dependencies and is unscheduled
        def schedulable(task):
            return task['depends_on'] == [] and task['status'] == Status.UNSCHEDULED

        logging.info("Starting scheduler...")
        while not self.task_pool.is_empty():
            # print(self.task_pool.tasks)
            # schedule only as many tasks as there are free slots
            tasks_to_schedule = self.task_pool.filter(schedulable).take(total_free_slots)
            # print(tasks_to_schedule.tasks)

            # if there are no free slots, wait till a free slot opens up
            # the schedule_event is set by a worker listener on receiving
            # a finished task
            if tasks_to_schedule.is_empty():
                self.schedule_event.clear()
                logging.info("Waiting for more schedulable tasks...")
                self.schedule_event.wait()
                tasks_to_schedule = self.task_pool \
                                        .filter(schedulable) \
                                        .take(total_free_slots)
            logging.debug(f"Scheduled: {[task['task_id'] for task in tasks_to_schedule.tasks]}")
            with self.scheduler_lock:
                task_map = self.scheduler(tasks_to_schedule.tasks, self.workers)
            status = self.send_task(task_map)
        logging.info(f"Done with job {job['job_id']}")

    def send_task(self, task_map_list):
        logging.info("Sending tasks to workers...")
        logging.debug(f"Mapped:\n{PrettyLog(task_map_list)}")
        for task_map in task_map_list:
            w_id = task_map['worker_id']
            conn, addr = self.connections[w_id]
            with conn:
                logging.debug(f"Sending to worker {w_id}:\n{PrettyLog(task_map['task'])}")
                message = json.dumps(task_map['task']).encode()
                conn.sendall(message)
                # reduce the number of free slots for the assigned workers
                self.update_worker_params(task_map, mode='decrement')
        return Status.SUCCESS

if __name__ == '__main__':
    # Context management protocol, all threads and connections
    # are automatically closed after it's done
    logging.basicConfig(filename='yacs_master.log', filemode='w', level=logging.DEBUG)
    with Master('config.json', Scheduler.random_scheduler) as master:
        master.run()
