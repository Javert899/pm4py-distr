from pm4pydistr.master.rqsts.basic_request import BasicMasterRequest
from pm4pydistr.configuration import KEYPHRASE
import requests
import json


class FilterRequest(BasicMasterRequest):
    def __init__(self, session, target_host, target_port, content, use_transition, no_samples):
        self.session = session
        self.target_host = target_host
        self.target_port = target_port
        self.content = content
        self.result = None
        BasicMasterRequest.__init__(self, session, target_host, target_port, content, use_transition, no_samples)

    def run(self):
        uri = "http://" + self.target_host + ":" + self.target_port + "/setFilters?keyphrase=" + KEYPHRASE + "&process=" + \
              self.content["process"] + "&session=" + str(self.session)

        r = requests.post(uri, data=json.dumps({"filters": self.content["data"]}))
