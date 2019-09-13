from pm4pydistr.remote_wrapper.distr_log_obj import DistrLogObj
from pm4pydistr.log_handlers import parquet as parquet_handler
from pm4py.objects.log.importer.parquet import factory as parquet_factory
from pathlib import Path
import requests
import json


class ClassicDistrLogObject(DistrLogObj):
    def __init__(self, hostname, port, keyphrase, log_name, parameters=None):
        DistrLogObj.__init__(self, hostname, port, keyphrase, log_name, parameters=parameters)

    def get_url(self, service):
        return "http://" + self.hostname + ":" + str(
            self.port) + "/" + service + "?keyphrase=" + self.keyphrase + "&process=" + self.log_name + "&session=" + str(
            self.session)

    def add_filter(self, filter_name, filter_value):
        self.filters.append([filter_name, filter_value])
        url = self.get_url("setFilters")
        print(url)
        r = requests.post(url, data={"filters": self.filters})
        return r.text

    def reset_filters(self):
        self.filters = None
        self.filters = []
        url = self.get_url("setFilters")
        r = requests.post(url, data={"filters": self.filters})
        return r.text

    def calculate_dfg(self, parameters=None):
        url = self.get_url("calculateDfg")
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        dfg = ret_json["dfg"]
        new_dfg = {}
        for el in dfg:
            new_dfg[(el.split("@@")[0], el.split("@@")[1])] = dfg[el]
        return new_dfg

    def get_end_activities(self):
        url = self.get_url("getEndActivities")
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["end_activities"]

    def get_start_activities(self):
        url = self.get_url("getStartActivities")
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["start_activities"]
