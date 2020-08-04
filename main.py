import os
from pm4pydistr.configuration import PYTHON_PATH
from threading import Thread
import subprocess

import time

class ExecutionThread(Thread):
    def __init__(self, command):
        self.command = command
        Thread.__init__(self)

    def run(self):
        os.system(self.command)
        #proc = subprocess.Popen(self.command, shell=True)
        #pid = proc.pid

t1 = ExecutionThread(PYTHON_PATH+" launch.py type master conf master port 7001")
t1.start()
