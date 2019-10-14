import psutil

for proc in psutil.process_iter():
    print(dir(proc))
    print(proc)

