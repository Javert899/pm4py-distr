import requests
import time
import json
import numpy as np

host = "137.226.117.71"
port = "5001"
service = "calculateDfg"
keyphrase = "hello"
process = "bigdistr0"

url = "http://"+host+":"+port+"/"+service+"?keyphrase="+keyphrase+"&process="+process
aa = time.time()
r = requests.get(url)
bb = time.time()
print(r.text)
#dfg = json.loads(r.text)["dfg"]
#print(np.sum(list(dfg.values())))
print("time required to call the service ",(bb-aa))