from pm4pydistr.master.rqsts.basic_request import BasicMasterRequest
from pm4pydistr.configuration import KEYPHRASE
import requests
import json


class EventsRequest(BasicMasterRequest):
    def __init__(self, session, target_host, target_port, use_transition, no_samples, content):
        self.session = session
        self.target_host = target_host
        self.target_port = target_port
        self.content = content
        self.use_transition = use_transition
        self.no_samples = no_samples
        self.case_id = None
        BasicMasterRequest.__init__(self, session, target_host, target_port, use_transition, no_samples, content)

    def run(self):
        uri = "http://" + self.target_host + ":" + self.target_port + "/getEvents?keyphrase=" + KEYPHRASE + "&process=" + str(
            self.content) + "&session=" + str(self.session) + "&use_transition=" + str(self.use_transition) + "&no_samples="+str(self.no_samples) + "&case_id="+str(self.case_id)
        r = requests.get(uri)
        self.content = json.loads(r.text)
