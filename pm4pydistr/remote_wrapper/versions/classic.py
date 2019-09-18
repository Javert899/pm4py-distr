from pm4pydistr.remote_wrapper.distr_log_obj import DistrLogObj
from pm4pydistr.log_handlers import parquet as parquet_handler
from pm4py.objects.log.importer.parquet import factory as parquet_factory
from pathlib import Path
from pm4pydistr.configuration import PARAMETER_USE_TRANSITION, DEFAULT_USE_TRANSITION
from pm4pydistr.configuration import PARAMETER_NO_SAMPLES, DEFAULT_MAX_NO_SAMPLES
import requests
import json


class ClassicDistrLogObject(DistrLogObj):
    def __init__(self, hostname, port, keyphrase, log_name, parameters=None):
        DistrLogObj.__init__(self, hostname, port, keyphrase, log_name, parameters=parameters)

    def get_url(self, service, parameters=None):
        if parameters is None:
            parameters = {}

        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]

        use_transition = parameters[PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
        no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES

        return "http://" + self.hostname + ":" + str(
            self.port) + "/" + service + "?keyphrase=" + self.keyphrase + "&process=" + self.log_name + "&session=" + str(
            self.session) + "&use_transition="+str(use_transition) + "&no_samples=" + str(no_samples)

    def do_log_assignment(self, parameters=None):
        url = self.get_url("doLogAssignment", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        return ret_text

    def add_filter(self, filter_name, filter_value, parameters=None):
        self.filters.append([filter_name, filter_value])
        url = self.get_url("setFilters", parameters=parameters)
        r = requests.post(url, json={"filters": json.dumps(self.filters)})
        return r.text

    def reset_filters(self, parameters=None):
        self.filters = None
        self.filters = []
        url = self.get_url("setFilters", parameters=parameters)
        r = requests.post(url, json={"filters": json.dumps(self.filters)})
        return r.text

    def calculate_dfg(self, parameters=None):
        url = self.get_url("calculateDfg", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        dfg = ret_json["dfg"]
        new_dfg = {}
        for el in dfg:
            new_dfg[(el.split("@@")[0], el.split("@@")[1])] = dfg[el]
        return new_dfg

    def get_end_activities(self, parameters=None):
        url = self.get_url("getEndActivities", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["end_activities"]

    def get_start_activities(self, parameters=None):
        url = self.get_url("getStartActivities", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["start_activities"]
