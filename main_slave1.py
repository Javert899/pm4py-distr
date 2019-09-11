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

t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --conf slave1 --port 5002 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t2 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --conf slave2 --port 5003 --master-host "+MASTER_HOST+" --master-port 5001")
t2.start()
t3 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --conf slave3 --port 5004 --master-host "+MASTER_HOST+" --master-port 5001")
t3.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --conf slave4 --port 5005 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --conf slave5 --port 5006 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --conf slave6 --port 5007 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --conf slave7 --port 5008 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --conf slave8 --port 5009 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --conf slave9 --port 5010 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --conf slave10 --port 5011 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --conf slave11 --port 5012 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --conf slave12 --port 5013 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()