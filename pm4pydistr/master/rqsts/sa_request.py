from pm4pydistr.master.rqsts.basic_request import BasicMasterRequest
from pm4pydistr.configuration import KEYPHRASE
import requests
import json


class SaRequest(BasicMasterRequest):
    def __init__(self, session, target_host, target_port, content):
        self.session = session
        self.target_host = target_host
        self.target_port = target_port
        self.content = content
        BasicMasterRequest.__init__(self, target_host, target_port, content)

    def run(self):
        uri = "http://" + self.target_host + ":" + self.target_port + "/getStartActivities?keyphrase=" + KEYPHRASE + "&process=" + str(
            self.content)
        r = requests.get(uri)
        self.content = json.loads(r.text)
