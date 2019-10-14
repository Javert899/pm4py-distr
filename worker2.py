import os
from pm4pydistr.configuration import PYTHON_PATH
from threading import Thread
import subprocess

import time

class ExecutionThread(Thread):
    def __init__(self, command, core):
        self.command = command
        self.core = core
        self.pid = None
        Thread.__init__(self)

    def run(self):
        #os.system(self.command)
        proc = subprocess.Popen(self.command, shell=True)
        self.pid = proc.pid
        os.system("taskset -cp "+str(self.core)+" "+str(self.pid))

MASTER_HOST = "137.226.117.71"
THIS_HOST = "137.226.117.74"

THIS_START = 2000
THIS_COUNT = 20

for i in range(THIS_COUNT):
    conf = "slave"+str(THIS_START+i+1)
    port = str(5000 + i + 2)
    t = ExecutionThread(PYTHON_PATH+" launch.py type slave host "+THIS_HOST+" conf "+conf+" port "+port+" masterhost "+MASTER_HOST+" masterport 5001", i)
    t.start()
