from pm4pydistr.master.rqsts.basic_request import BasicMasterRequest
from pm4pydistr.configuration import KEYPHRASE
import requests
import json

class MasterAssignRequest(BasicMasterRequest):
    def __init__(self, session, target_host, target_port, use_transition, no_samples, content):
        self.slave_finished = 0
        self.session = session
        self.target_host = target_host
        self.target_port = target_port
        self.content = content
        self.use_transition = use_transition
        self.no_samples = no_samples
        BasicMasterRequest.__init__(self, None, target_host, target_port, use_transition, no_samples, content)

    def run(self):
        uri = "http://"+self.target_host+":"+self.target_port+"/synchronizeFiles?keyphrase="+KEYPHRASE + "&use_transition=" + str(self.use_transition) + "&no_samples="+str(self.no_samples)

        r = requests.post(uri, data=json.dumps(self.content))

        self.slave_finished = 1
