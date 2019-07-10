from pm4pydistr.master.rqsts.basic_request import BasicMasterRequest
from pm4pydistr.configuration import KEYPHRASE
import requests
import json

class MasterAssignRequest(BasicMasterRequest):
    def __init__(self, target_host, target_port, content):
        self.target_host = target_host
        self.target_port = target_port
        self.content = content
        self.result = None
        BasicMasterRequest.__init__(self, target_host, target_port, content)

    def run(self):
        uri = "http://"+self.target_host+":"+self.target_port+"/synchronizeFiles?keyphrase="+KEYPHRASE

        print(uri)
        print(self.content)

        r = requests.post(uri, data=json.dumps(self.content))

