from pm4pydistr.master.rqsts.basic_request import BasicMasterRequest
from pm4pydistr.configuration import KEYPHRASE
import requests
import json


class EaRequest(BasicMasterRequest):
    def __init__(self, session, target_host, target_port, content, use_transition, no_samples):
        self.session = session
        self.target_host = target_host
        self.target_port = target_port
        self.content = content
        BasicMasterRequest.__init__(self, session, target_host, target_port, content, use_transition, no_samples)

    def run(self):
        uri = "http://" + self.target_host + ":" + self.target_port + "/getEndActivities?keyphrase=" + KEYPHRASE + "&process=" + str(
            self.content) + "&session=" + str(self.session)
        r = requests.get(uri)
        self.content = json.loads(r.text)
