from threading import Thread
from queue import Queue
from enum import Enum
import socket
import json
import random
import time
import sys
'''task={}
                    task['job_id']:job['id']
                    task['task_id']:mapper['task_id']
                    task['duration']:mapper['duration']
                    task['wid']:None
                    task['depends']:None
                    task['satisfies']:redtasks
                    task['status']:Status(2)'''
port=sys.argv[1]
wid=sys.argv[2]
def sleep(task):
    #task['wid']=wid
    duration=task['duration']
    time.sleep(duration)#sleeps for duration
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", 5001))
        message=json.dumps(task)#sends back the task to worker
        s.send(message.encode())

def perform_task(s):
        s.bind('localhost',port)
        s.listen(5)
        while(1):
            connection,addr=s.accept()

            while(1):
                message=connection.recv(1024)
                task=message.decode("utf-8")
                task=json.loads(task)
                slot = Thread(target=sleep, args=(task),)#individual thread for each task
                slot.start()
                #time.sleep(task['duration'])


if __name__ == '__main__':  
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener = Thread(target=perform_task, args=(s),)#thread to listen for jobs
    listener.start()

