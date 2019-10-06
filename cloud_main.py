import os
from pm4pydistr.configuration import PYTHON_PATH
from threading import Thread
import multiprocessing

class ExecutionThread(Thread):
    def __init__(self, command):
        self.command = command
        Thread.__init__(self)

    def run(self):
        os.system(self.command)


t1 = ExecutionThread(PYTHON_PATH+" launch.py type master conf master port 5001")
t1.start()
no_cores = multiprocessing.cpu_count()
for i in range(no_cores-1):
    current_sc = "slave%d" % (i+1)
    current_port = str(5002+i)
    t2 = ExecutionThread(PYTHON_PATH + " launch.py type slave port "+str(current_port)+" conf "+current_sc)
    t2.start()
