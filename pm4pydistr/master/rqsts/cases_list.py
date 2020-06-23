from pm4pydistr.master.rqsts.basic_request import BasicMasterRequest
from pm4pydistr.configuration import KEYPHRASE, DEFAULT_WINDOW_SIZE, PARAMETER_NUM_RET_ITEMS
import requests
import json


class CasesListRequest(BasicMasterRequest):
    def __init__(self, session, target_host, target_port, use_transition, no_samples, content):
        self.session = session
        self.target_host = target_host
        self.target_port = target_port
        self.content = content
        self.use_transition = use_transition
        self.no_samples = no_samples
        self.max_ret_items = DEFAULT_WINDOW_SIZE
        BasicMasterRequest.__init__(self, session, target_host, target_port, use_transition, no_samples, content)

    def run(self):
        uri = "http://" + self.target_host + ":" + self.target_port + "/getCases?keyphrase=" + KEYPHRASE + "&process=" + str(
            self.content) + "&session=" + str(self.session) + "&use_transition=" + str(self.use_transition) + "&no_samples=" + str(self.no_samples) + "&" + PARAMETER_NUM_RET_ITEMS + "=" + str(self.max_ret_items)
        r = requests.get(uri)
        self.content = json.loads(r.text)
