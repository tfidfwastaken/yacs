"""Worker"""

from queue import SimpleQueue
from threading import Event, Lock, Thread
from utils import Status
import csv
import json
import logging
import pathlib
import socket
import struct
import sys
import time

# Status enum provided by utils.py
from utils import Status


class Worker:
    def __init__(self, conn_info, worker_id, slot_count):
        self.master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.id = worker_id
        self.conn_info = conn_info
        self.exec_pool = []

        self.exit_command_received = Event()
        self.tasks_available = Event()
        self.exec_pool_lock = Lock()

        self.task_log = {}

    def __enter__(self):
        logging.info("Starting Worker...")
        self.master_sock.bind(self.conn_info)
        self.task_listener_thread = Thread(name="TaskListener", \
                                           target=self.task_listener, \
                                           args=(self.master_sock,))
        self.task_listener_thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logging.info("Exiting...")
        self.exit_command_received.set()
        self.master_sock.shutdown(socket.SHUT_RD)
        self.master_sock.close()
        logging.debug("Closed connection sockets")
        self.task_listener_thread.join()
        logging.debug("Closed threads")

    # connects to master and listens for tasks
    def task_listener(self, master):
        master.connect(('localhost', 5001))
        print("Connected to master.")
        logging.info("Connection to master successful")
        while not self.exit_command_received.is_set():
            logging.info("Listening for tasks")
            task_message = master.recv(4096).decode()
            if not task_message:
                logging.error("Connection to master broken")
                self.exit_command_received.set()
                break
            task = json.loads(task_message)
            logging.info(f"Received task: {task['task_id']} of job {task['job_id']} at {time.time()}")
            self.task_log[task['task_id']]={}
            self.task_log[task['task_id']]['job_id'] = task['job_id']
            self.task_log[task['task_id']]['start'] = time.time()
            with self.exec_pool_lock:
                self.exec_pool.append(task)
            # notify main thread that there are tasks available
            self.tasks_available.set()

    # Informs the master on task success
    def send_task(self, task):
        task['status'] = Status.SUCCESS
        task_map = {'worker_id': self.id, 'task': task}
        message = json.dumps(task_map).encode()
        message = struct.pack('!I', len(message)) + message
        self.master_sock.sendall(message)
        return Status.SUCCESS

    # To record the start and finish time of task in csv
    def record_log(self, tid):
        with open(r'worker_'+str(self.id)+'.csv', 'a') as f:
            writer = csv.writer(f)
            jid = self.task_log[tid]['job_id']
            start = self.task_log[tid]['start']
            stop = self.task_log[tid]['end']
            writer.writerow([jid,tid,start,stop])

    # starts the worker
    def run(self):
        logging.info("Waiting for tasks")
        while not self.exit_command_received.is_set():
            completed_tasks = []
            self.tasks_available.wait()
            with self.exec_pool_lock:
                for task in self.exec_pool:
                    task['duration'] -= 1
                    if task['duration'] <= 0:
                        self.exec_pool.remove(task)
                        completed_tasks.append(task)
                        logging.info(f"Finished tasks: {task['task_id']} of job {task['job_id']} at {time.time()}")
                        self.task_log[task['task_id']]['end'] = time.time()
                        self.record_log(task['task_id'])
            time.sleep(1)
            if self.exec_pool == []:
                self.tasks_available.wait()
            logging.info(f"Reporting back to Master...")
            for task in completed_tasks:
                status = self.send_task(task)
        print("Master has exited. Exiting in 10 seconds...")
        time.sleep(10)

if __name__ == '__main__':
    port = int(sys.argv[1])
    worker_id = int(sys.argv[2])
    config_location = pathlib.Path(__file__).absolute().parent.parent / 'config.json'
    with open(config_location) as cfg:
        workers = json.load(cfg)['workers']

    logging.basicConfig(filename=f'yacs_worker_{worker_id}.log', \
                        filemode='w', level=logging.DEBUG)
    f = open(r'worker_'+str(worker_id)+'.csv', 'w')
    # make this more robust later
    this_worker = None
    for worker in workers:
        if worker['worker_id'] == worker_id:
            this_worker = worker

    with Worker(('localhost', port), worker_id, this_worker['slots']) as worker:
        worker.run()
    f.close()
