from pm4pydistr.master.rqsts.basic_request import BasicMasterRequest
from pm4pydistr.configuration import KEYPHRASE
import requests
import json


class CorrMiningRequest(BasicMasterRequest):
    def __init__(self, session, target_host, target_port, use_transition, no_samples, process, json_content):
        self.session = session
        self.target_host = target_host
        self.target_port = target_port
        self.json_content = json_content
        self.use_transition = use_transition
        self.no_samples = no_samples
        self.process = process

        self.PS_matrix = []
        self.duration_matrix = []

        BasicMasterRequest.__init__(self, session, target_host, target_port, use_transition, no_samples, content)

    def run(self):
        uri = "http://" + self.target_host + ":" + self.target_port + "/correlationMiner?keyphrase=" + KEYPHRASE + "&process=" + str(
            self.process) + "&session=" + str(self.session) + "&use_transition=" + str(
            self.use_transition) + "&no_samples=" + str(self.no_samples)
        print(self.json_content)
        r = requests.post(uri, json=self.json_content)
        resp = json.loads(r.text)
        self.PS_matrix = resp["PS_matrix"]
        self.duration_matrix = resp["duration_matrix"]
