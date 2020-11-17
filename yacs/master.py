"""Master node

This program runs on the master.
"""
from threading import Thread
from queue import SimpleQueue
from enum import Enum
import socket
import json

class Status(Enum):
    SUCCESS = 1
    PENDING = 2
    FAILED = 3


class Scheduler:
    @staticmethod
    def random_scheduler(tasks, workers):
        pass

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
        self.workers = config['Workers']
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
        pass

    def worker_listener(self, conn):
        pass

    def update_worker_params(self, worker):
        pass
    
    def run(self):
        pass

    def start_job(self):
        pass

    def send_task(self, task_mapping, conn):
        pass

if __name__ == '__main__':
    # Context management protocol, all threads and connections
    # are automatically closed after it's done
    with Master() as master:
        master.run()
