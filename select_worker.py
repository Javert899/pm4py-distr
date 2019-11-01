import socket
import time
import os
ip_address = str(socket.gethostbyname(socket.gethostname()))

if ip_address == "137.226.117.71":
    os.system("nohup python3 main.py &")
    time.sleep(15)
    os.system("nohup python3 worker6.py &")
elif ip_address == "137.226.117.72":
    time.sleep(20)
    os.system("nohup python3 worker5.py &")
elif ip_address == "137.226.117.73":
    time.sleep(20)
    os.system("nohup python3 worker1.py &")
elif ip_address == "137.226.117.74":
    time.sleep(20)
    os.system("nohup python3 worker2.py &")
elif ip_address == "137.226.117.75":
    time.sleep(20)
    os.system("nohup python3 worker3.py &")
elif ip_address == "137.226.117.76":
    time.sleep(20)
    os.system("nohup python3 worker4.py &")
time.sleep(3)
os.system("nohup python3 set_task_priority.py &")