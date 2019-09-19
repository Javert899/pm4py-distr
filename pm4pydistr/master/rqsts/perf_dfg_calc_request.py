from pm4pydistr.master.rqsts.basic_request import BasicMasterRequest
from pm4pydistr.configuration import KEYPHRASE
import requests
import json
from pm4py.objects.log.util import xes


class PerfDfgCalcRequest(BasicMasterRequest):
    def __init__(self, session, target_host, target_port, use_transition, no_samples, content):
        self.session = session
        self.target_host = target_host
        self.target_port = target_port
        self.content = content
        self.use_transition = use_transition
        self.no_samples = no_samples
        self.attribute_key = xes.DEFAULT_NAME_KEY

        BasicMasterRequest.__init__(self, session, target_host, target_port, use_transition, no_samples, content)

    def run(self):
        uri = "http://" + self.target_host + ":" + self.target_port + "/calculatePerformanceDfg?keyphrase=" + KEYPHRASE + "&process=" + str(
            self.content) + "&session=" + str(self.session) + "&use_transition=" + str(self.use_transition) + "&no_samples="+str(self.no_samples)+"&attribute_key="+str(self.attribute_key)

        r = requests.get(uri)
        self.content = json.loads(r.text)
