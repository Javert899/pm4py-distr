import requests
import time


host = "137.226.117.71"
port = "5001"
service = "calculateDfg"
keyphrase = "hello"
process = "roadtraffic"

url = "http://"+host+":"+port+"/"+service+"?keyphrase="+keyphrase+"&process="+process
aa = time.time()
r = requests.get(url)
bb = time.time()
print(r.text)
print("time required to calculate the DFG ",(bb-aa))