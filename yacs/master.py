"""Master node

This program runs on the master.
"""

from threading import Thread, Event, Lock
from queue import SimpleQueue
import logging
import sys
import random
from signal import signal, SIGINT
import socket
import json

from utils import TaskPool, Status


class Scheduler:
    @staticmethod
    def random_scheduler(tasks, workers):
        task_mapping = []
        for task in tasks:
            while True:
                worker = random.choice(workers)
                if worker['free_slot_count'] > 0:
                    task_mapping.append({
                        'worker_id': worker['worker_id'],
                        'task': task
                    })
                    break
        return task_mapping
                    
    @staticmethod
    def round_robin_scheduler(tasks, workers):
        pass

    @staticmethod
    def least_loaded_scheduler(tasks, workers):
        pass


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
                logging.debug(f"got request: {req}")
                job = json.loads(req)
                self.job_pool.put(job)
                # logging.debug(f"Job Pool: {[job['job_id'] for job in list(job_pool.queue)]}")

    def worker_listener(self, conn_info):
        conn, addr = conn_info
        while not self.exit_command_received.is_set():
            with conn:
                logging.info(f"Listening for messages from {addr}")
                worker_message = conn.recv(4096).decode()
                if not worker_message:
                    logging.error(f"Connection from worker {addr} broken")
                    break
                logging.debug(f"Received from worker {addr}: {worker_message}")
                task_map = json.loads(worker_message)
                self.update_worker_params(task_map)
                # notify the scheduler that there are free slots
                self.schedule_event.set()

    def update_worker_params(self, task_map):
        pass
    
    def run(self):
        while True:
            # read from job pool
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

    def send_task(self, task_mapping):
        logging.info("Sending tasks to workers...")
        logging.debug(f"Mapped: {task_mapping}")
        for mapping in task_mapping:
            w_id = mapping['worker_id']
            conn, addr = self.connections[w_id]
            with conn:
                logging.debug(f"Sending to worker {w_id}: {mapping['task']}")
                message = json.dumps(mapping['task']).encode()
                conn.sendall(message)
                self.update_worker_params(mapping)
        # TODO: remove from task pool
        return Status.SUCCESS

if __name__ == '__main__':
    # Context management protocol, all threads and connections
    # are automatically closed after it's done
    logging.basicConfig(filename='yacs_master.log', filemode='w', level=logging.DEBUG)
    with Master('config.json', Scheduler.random_scheduler) as master:
        master.run()
