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
THIS_HOST = "137.226.117.76"

t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave37 --port 5002 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t2 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave38 --port 5003 --master-host "+MASTER_HOST+" --master-port 5001")
t2.start()
t3 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave39 --port 5004 --master-host "+MASTER_HOST+" --master-port 5001")
t3.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave40 --port 5005 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave41 --port 5006 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave42 --port 5007 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave43 --port 5008 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave44 --port 5009 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave45 --port 5010 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave46 --port 5011 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave47 --port 5012 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave48 --port 5013 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()