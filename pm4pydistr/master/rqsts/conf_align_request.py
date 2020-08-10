from pm4pydistr.master.rqsts.basic_request import BasicMasterRequest
from pm4pydistr.configuration import KEYPHRASE
import requests
import json
import sys


class AlignRequest(BasicMasterRequest):
    def __init__(self, session, target_host, target_port, use_transition, no_samples, process, content):
        self.session = session
        self.target_host = target_host
        self.target_port = target_port
        self.content = content
        self.use_transition = use_transition
        self.no_samples = no_samples
        self.process = process
        BasicMasterRequest.__init__(self, None, target_host, target_port, use_transition, no_samples, content)

    def run(self):
        try:
            uri = "http://" + self.target_host + ":" + self.target_port + "/performAlignments?keyphrase=" + KEYPHRASE + "&use_transition=" + str(
                self.use_transition) + "&no_samples=" + str(self.no_samples) + "&process=" + str(
                self.process)

            r = requests.post(uri, data=json.dumps(self.content))
            self.content = json.loads(r.text)
        except:
            self.content = r.text
            raise Exception(r.text)
