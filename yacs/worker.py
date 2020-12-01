from threading import Thread
from queue import Queue
from enum import Enum
import socket
import json
import random
import time
import sys

port=int(sys.argv[1])
wid=sys.argv[2]
def pretend_sleep(task):
    task['wid']=wid
    duration=task['duration']

    print("\nI'm worker",wid,"Got some work...")
    print(task)
    time.sleep(duration)#sleeps for duration
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as skt:
        skt.connect(("localhost", 5001))
        message=json.dumps(task)#sends back the task to worker
        skt.send(message.encode())
        print("Completion Message sent!\n")

def perform_task(s):
    s.bind(('localhost',port))
    s.listen(5)
    while(1):
        connection,addr=s.accept()
        print("Connection accepted!")
        message=connection.recv(1024)
        if(len(message)!=0):
            task=message.decode()
            task=json.loads(task)
            slot = Thread(target=pretend_sleep, args=(task,))#individual thread for each task
            slot.start()
            #time.sleep(task['duration'])
if __name__ == '__main__':
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener = Thread(target=perform_task, args=(s,))#thread to listen for jobs
    listener.start()
