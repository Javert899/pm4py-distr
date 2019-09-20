import requests
import json

from pm4pydistr import configuration


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
        r = requests.get(
            "http://" + self.master_host + ":" + self.master_port + "/registerSlave?keyphrase=" + configuration.KEYPHRASE + "&ip=" + self.host + "&port=" + self.port + "&conf=" + self.conf)

        response = json.loads(r.text)
        self.id = response['id']
        self.slave.id = response['id']

        self.slave.enable_ping_of_master()
