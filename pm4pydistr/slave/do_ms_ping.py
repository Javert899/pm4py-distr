from threading import Thread
import time
from pm4pydistr import configuration
import requests


class DoMasterPing(Thread):
    def __init__(self, slave, conf, id, master_host, master_port):
        self.slave = slave
        self.conf = conf
        self.id = str(id)
        self.master_host = master_host
        self.master_port = master_port
        Thread.__init__(self)

    def run(self):
        while True:
            uri = "http://"+self.master_host+":"+self.master_port+"/pingFromSlave?id="+str(self.id)+"&conf="+str(self.conf)+"&keyphrase="+configuration.KEYPHRASE
            r = requests.get(uri)
            #print("done ping request")
            time.sleep(configuration.SLEEPING_TIME)
