from pm4pydistr.local_wrapper.distr_log_obj import LocalDistrLogObj
from pm4pydistr.log_handlers import parquet as parquet_handler
from pm4py.objects.log.importer.parquet import factory as parquet_factory
from pm4py.util import constants as pm4py_constants
from pathlib import Path
from copy import deepcopy


class ClassicDistrLogObject(LocalDistrLogObj):
    def __init__(self, distr_log_path, parameters=None):
        LocalDistrLogObj.__init__(self, distr_log_path, parameters=parameters)

    def get_list_logs(self):
        lp = parquet_factory.get_list_parquet(self.distr_log_path)

        return [Path(log).name for log in lp]

    def add_filter(self, filter_name, filter_value, parameters=None):
        if parameters is None:
            parameters = {}
        self.filters.append([filter_name, filter_value])

    def reset_filters(self, parameters=None):
        if parameters is None:
            parameters = {}
        self.filters = []

    def calculate_dfg(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters
        dfg = parquet_handler.calculate_dfg(".", self.distr_log_path, list_logs, parameters=parameters)
        return {(x.split("@@")[0], x.split("@@")[1]): dfg[x] for x in dfg}

    def get_end_activities(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters
        end_activities = parquet_handler.get_end_activities(".", self.distr_log_path, list_logs, parameters=parameters)
        return end_activities

    def get_start_activities(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters
        start_activities = parquet_handler.get_start_activities(".", self.distr_log_path, list_logs,
                                                                parameters=parameters)
        return start_activities

    def get_log_summary(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters

        dictio = parquet_handler.get_log_summary(".", self.distr_log_path, list_logs,
                                                                parameters=parameters)

        return dictio

    def get_attribute_values(self, attribute_key, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters
        parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = attribute_key

        dictio = parquet_handler.get_attribute_values(".", self.distr_log_path, list_logs, parameters=parameters)

        return dictio

    def get_attribute_names(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters

        names = parquet_handler.get_attribute_names(".", self.distr_log_path, list_logs, parameters=parameters)

        return names


def apply(path, parameters=None):
    if parameters is None:
        parameters = {}

    return ClassicDistrLogObject(path, parameters=parameters)
