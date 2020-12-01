from threading import Thread
from queue import Queue
from multiprocessing import SimpleQueue
from enum import Enum
import socket
import json
import random
import time
import sys

def worker_listener(connection,addr):
    while(1):
        message=connection.recv(1024)
        if(len(message)!=0):
            print("FROM : ",addr,"\n")
            print("Message says :\n",message.decode())

print("Listening to workers now...")
s =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
port = 5001
s.bind(('localhost',port))
s.listen(5)
new_conn = []
while(1):
    connection,addr = s.accept()
    print("Connection:\n",connection,"Address :\n",addr)
    if addr not in new_conn:
        new_conn.append(addr)
        th = Thread(target=worker_listener, args=(connection,addr,))
        th.start()
