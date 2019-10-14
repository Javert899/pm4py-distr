import psutil
import os


cpu_count = 0
for proc in psutil.process_iter():
    name = " ".join(proc.cmdline())
    if name.startswith("python3 launch.py "):
        os.system("taskset -cp "+str(cpu_count)+" "+str(proc.pid))
        os.system("renice -20 -p "+str(proc.pid))
        cpu_count = cpu_count + 1
print("allocated cpu=",cpu_count)
