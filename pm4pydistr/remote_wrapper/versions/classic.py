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
        raise Exception("not implemented")

    def reset_filters(self):
        raise Exception("not implemented")

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
        raise Exception("not implemented")

    def get_start_activities(self):
        raise Exception("not implemented")
