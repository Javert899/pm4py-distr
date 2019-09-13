from pm4pydistr.remote_wrapper.distr_log_obj import DistrLogObj
from pm4pydistr.log_handlers import parquet as parquet_handler
from pm4py.objects.log.importer.parquet import factory as parquet_factory
from pathlib import Path


class ClassicDistrLogObject(DistrLogObj):
    def __init__(self, hostname, port, log_name, parameters=None):
        DistrLogObj.__init__(hostname, port, log_name, parameters=parameters)

    def add_filter(self, filter_name, filter_value):
        raise Exception("not implemented")

    def reset_filters(self):
        raise Exception("not implemented")

    def calculate_dfg(self, parameters=None):
        raise Exception("not implemented")

    def get_end_activities(self):
        raise Exception("not implemented")

    def get_start_activities(self):
        raise Exception("not implemented")
