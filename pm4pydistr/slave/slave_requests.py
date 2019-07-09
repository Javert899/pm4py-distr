import requests
import json

from pm4pydistr.configuration import KEYPHRASE


class SlaveRequests:
    def __init__(self, slave, host, port, master_host, master_port, conf):
        self.slave = slave

        self.host = host
        self.port = port
        self.master_host = master_host
        self.master_port = master_port
        self.conf = conf
        self.id = None

    def register_to_webservice(self):
        r = requests.get("http://"+self.master_host+":"+self.master_port+"/registerSlave?keyphrase="+KEYPHRASE+"&ip="+self.host+"&port="+self.port)

        response = json.loads(r.text)
        self.id = response['id']
        self.slave.id = response['id']

