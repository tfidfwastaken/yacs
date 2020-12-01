"""Worker"""

from threading import Thread, Event
from queue import SimpleQueue, Empty
import logging
import sys
import socket
import json

from utils import Status


class Worker:
    def __init__(self, conn_info, slot_count):
        self.master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conn_info = conn_info
        self.exec_pool = SimpleQueue()
        self.slot_count = slot_count
        self.exit_command_received = Event()

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
            task_message = master.recv(4096).decode()
            if not task_message:
                logging.error("Connection to master broken")
                self.exit_command_received.set()
                break
            task = json.loads(task_message)
            logging.debug(f"Received task: {task['task_id']} of job {task['job_id']}")
            self.exec_pool.put(task)

    def send_task(self, task):
        pass
    
    def run(self):
        while not self.exit_command_received.is_set():
            # get them tasks
            logging.info("Waiting for tasks")
            try:
                task = self.exec_pool.get(timeout=10)
            except Empty:
                if self.exit_command_received.is_set():
                    break
                else:
                    continue
            task['duration'] -= 1
            if task['duration'] != 0:
                self.exec_pool.put(task)
            else:
                logging.info(f"Task {task['task_id']} completed! Sending to Master...")
                status = self.send_task(task)

if __name__ == '__main__':
    logging.basicConfig(filename='yacs_worker.log', filemode='w', level=logging.DEBUG)
    port = int(sys.argv[1])
    worker_id = int(sys.argv[2])
    with open('config.json') as cfg:
        workers = json.load(cfg)['workers']

    # make this more robust
    this_worker = None
    for worker in workers:
        if worker['worker_id'] == worker_id:
            this_worker = worker

    with Worker(('localhost', port), this_worker['slots']) as worker:
        worker.run()
