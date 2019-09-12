import os
from pm4pydistr.configuration import PYTHON_PATH
from threading import Thread

import time

class ExecutionThread(Thread):
    def __init__(self, command):
        self.command = command
        Thread.__init__(self)

    def run(self):
        os.system(self.command)

MASTER_HOST = "137.226.117.71"
THIS_HOST = "137.226.117.72"

THIS_START = 5000
THIS_COUNT = 20

for i in range(THIS_COUNT):
    conf = "slave"+str(THIS_START+i+1)
    port = str(5000 + i + 2)
    t = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf "+conf+" --port "+port+" --master-host "+MASTER_HOST+" --master-port 5001")
    t.start()
