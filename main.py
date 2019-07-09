import os
from pm4pydistr.configuration import PYTHON_PATH
from threading import Thread


class ExecutionThread(Thread):
    def __init__(self, command):
        self.command = command
        Thread.__init__(self)

    def run(self):
        os.system(self.command)

t1 = ExecutionThread(PYTHON_PATH+" launch.py --type master --conf master --port 5001")
t1.start()
t2 = ExecutionThread(PYTHON_PATH+" launch.py --type slave1 --conf slave1 --port 5002")
t2.start()
