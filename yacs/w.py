from threading import Thread
from queue import Queue
from multiprocessing import SimpleQueue
from enum import Enum
import socket
import json
import random
import time
import sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 5001))
for i in range(1,11):
    message="DONE - " + str(i)
    s.send(message.encode())
    print("Completion Message sent!")
    time.sleep(3)
