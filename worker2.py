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
THIS_HOST = "137.226.117.74"

t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave13 --port 5002 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t2 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave14 --port 5003 --master-host "+MASTER_HOST+" --master-port 5001")
t2.start()
t3 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave15 --port 5004 --master-host "+MASTER_HOST+" --master-port 5001")
t3.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave16 --port 5005 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave17 --port 5006 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave18 --port 5007 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave19 --port 5008 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave20 --port 5009 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave21 --port 5010 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave22 --port 5011 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave23 --port 5012 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()
t1 = ExecutionThread(PYTHON_PATH+" launch.py --type slave --host "+THIS_HOST+" --conf slave24 --port 5013 --master-host "+MASTER_HOST+" --master-port 5001")
t1.start()