import os
from pm4pydistr.configuration import PYTHON_PATH
from threading import Thread
import multiprocessing

import time

class ExecutionThread(Thread):
    def __init__(self, command):
        self.command = command
        Thread.__init__(self)

    def run(self):
        os.system(self.command)

t1 = ExecutionThread(PYTHON_PATH+" launch.py type master conf master port 5001")
t1.start()

N = multiprocessing.cpu_count()

for i in range(N):
	t = ExecutionThread(PYTHON_PATH+" launch.py type slave conf slave"+str(i)+" autoport 1 masterhost 127.0.0.1 masterport 5001")
	t.start()
