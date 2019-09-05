from pm4pydistr.master.rqsts.basic_request import BasicMasterRequest
from pm4pydistr.configuration import KEYPHRASE
import requests
import json


class FilterRequest(BasicMasterRequest):
    def __init__(self, target_host, target_port, content):
        self.target_host = target_host
        self.target_port = target_port
        self.content = content
        self.result = None
        BasicMasterRequest.__init__(self, target_host, target_port, content)

    def run(self):
        uri = "http://"+self.target_host+":"+self.target_port+"/setFilters?keyphrase="+KEYPHRASE+"&process="+self.content["process"]

        r = requests.post(uri, data=json.dumps({"filters": self.content["data"]}))
