"""Worker"""

from threading import Thread, Event
from queue import SimpleQueue, Empty
from utils import Status
import logging
import sys
import socket
import json
import time
from utils import Status
import csv

class Worker:
    def __init__(self, conn_info, worker_id, slot_count):
        self.master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.id = worker_id
        self.conn_info = conn_info
        self.exec_pool = SimpleQueue()
        self.slot_count = slot_count
        self.exit_command_received = Event()
        self.task_log = dict()
        self.file = open('worker_'+str(self.id)+'.csv', 'w', newline='')

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

    def task_listener(self, master):
        master.connect(('localhost', 5001))
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

            # logging.debug(f"Received task: {task['task_id']} of job {task['job_id']}")
            self.exec_pool.put(task)

    def send_task(self, task):
        task['status'] = Status.SUCCESS
        task_map = {'worker_id': self.id, 'task': task}
        message = json.dumps(task_map).encode()
        self.master_sock.sendall(message)
        return Status.SUCCESS

    # To record the start and finish time of task in csv
    def record_log(self,tid):
        with open(r'worker_'+str(self.id)+'.csv', 'a') as f:
            writer = csv.writer(f)
            jid = self.task_log[tid]['job_id']
            start = self.task_log[tid]['start']
            stop = self.task_log[tid]['end']
            writer.writerow([jid,tid,start,stop])

    def run(self):
        logging.info("Waiting for tasks")
        while not self.exit_command_received.is_set():
            # get them tasks
            try:
                task = self.exec_pool.get(timeout=10)
            except Empty:
                if self.exit_command_received.is_set():
                    break
                else:
                    continue
            time.sleep(1)
            task['duration'] -= 1
            if task['duration'] != 0:
                self.exec_pool.put(task)
            else:
                logging.info(f"Finished task: {task['task_id']} of job {task['job_id']} at {time.time()}")
                self.task_log[task['task_id']]['end'] = time.time()
                self.record_log(task['task_id'])

                logging.info(f"Task {task['task_id']} completed! Sending to Master...")
                status = self.send_task(task)


if __name__ == '__main__':
    port = int(sys.argv[1])
    worker_id = int(sys.argv[2])
    with open('config.json') as cfg:
        workers = json.load(cfg)['workers']

    logging.basicConfig(filename=f'yacs_worker_{worker_id}.log', \
                        filemode='w', level=logging.DEBUG)
    this_worker = None
    for worker in workers:
        if worker['worker_id'] == worker_id:
            this_worker = worker

    with Worker(('localhost', port), worker_id, this_worker['slots']) as worker:
        worker.run()
