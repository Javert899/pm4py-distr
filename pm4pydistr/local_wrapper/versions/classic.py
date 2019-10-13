from pm4pydistr.local_wrapper.distr_log_obj import LocalDistrLogObj
from pm4pydistr.log_handlers import parquet as parquet_handler
from pm4py.objects.log.importer.parquet import factory as parquet_factory
from pm4py.util import constants as pm4py_constants
from pathlib import Path
from pm4py.algo.filtering.common.attributes import attributes_common
from pm4py.statistics.traces.common import case_duration as case_duration_commons
from datetime import datetime
from pm4py.objects.petri.exporter.versions import pnml as pnml_exporter
from pm4py.algo.filtering.log.variants import variants_filter as log_variants_filter
from pm4pydistr.slave import slave


class ClassicDistrLogObject(LocalDistrLogObj):
    def __init__(self, distr_log_path, parameters=None):
        LocalDistrLogObj.__init__(self, distr_log_path, parameters=parameters)

    def do_caching(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parquet_handler.do_caching(".", self.distr_log_path, list_logs, parameters=parameters)

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

    def calculate_performance_dfg(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters
        dfg = parquet_handler.calculate_performance_dfg(".", self.distr_log_path, list_logs, parameters=parameters)
        return {(x.split("@@")[0], x.split("@@")[1]): dfg[x] for x in dfg}

    def calculate_composite_object(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters
        obj = parquet_handler.calculate_process_schema_composite_object(".", self.distr_log_path, list_logs, parameters=parameters)
        new_frequency = {}
        for el in obj["frequency_dfg"]:
            new_frequency[(el.split("@@")[0], el.split("@@")[1])] = obj["frequency_dfg"][el]
        obj["frequency_dfg"] = new_frequency
        if "performance_dfg" in obj:
            new_performance = {}
            for el in obj["performance_dfg"]:
                new_performance[(el.split("@@")[0], el.split("@@")[1])] = obj["performance_dfg"][el]
            obj["performance_dfg"] = new_performance
        return obj

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


    def get_variants(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters

        variants = parquet_handler.get_variants(".", self.distr_log_path, list_logs, parameters=parameters)

        return variants

    def get_cases(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters

        cases = parquet_handler.get_cases(".", self.distr_log_path, list_logs, parameters=parameters)

        return cases

    def get_events(self, case_id, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters
        parameters["case_id"] = case_id

        events = parquet_handler.get_events(".", self.distr_log_path, list_logs, parameters=parameters)

        return events

    def get_events_per_dotted(self, attribute1, attribute2, attribute3, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters
        parameters["attribute1"] = attribute1
        parameters["attribute2"] = attribute2
        parameters["attribute3"] = attribute3

        ret = parquet_handler.get_events_per_dotted(".", self.distr_log_path, list_logs, parameters=parameters)

        return {"traces": ret[0], "types": ret[1], "attributes": ret[2], "third_unique_values": ret[3]}

    def get_events_per_time(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters

        ret = parquet_handler.get_events_per_time(".", self.distr_log_path, list_logs, parameters=parameters)
        ret = [datetime.fromtimestamp(x) for x in ret]

        x, y = attributes_common.get_kde_date_attribute(ret)

        return x, y

    def get_case_duration(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters

        ret = parquet_handler.get_case_duration(".", self.distr_log_path, list_logs, parameters=parameters)

        x, y = case_duration_commons.get_kde_caseduration(ret)

        return x, y

    def get_numeric_attribute(self, attribute_key, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters
        parameters["attribute_key"] = attribute_key

        ret = parquet_handler.get_case_duration(".", self.distr_log_path, list_logs, parameters=parameters)

        x, y = attributes_common.get_kde_numeric_attribute(ret)

        return x, y

    def perform_alignments_net_log(self, net, im, fm, log, parameters=None):
        if parameters is None:
            parameters = {}
        variants = log_variants_filter.get_variants_from_log_trace_idx(log, parameters=parameters)
        var_list = [[x, y] for x,y in variants.items()]

        result = self.perform_alignments_net_variants(net, im, fm, var_list=var_list, parameters=parameters)

        al_idx = {}
        for index_variant, variant in enumerate(variants):
            for trace_idx in variants[variant]:
                al_idx[trace_idx] = result[variant]

        alignments = []
        for i in range(len(log)):
            alignments.append(al_idx[i])

        return alignments

    def perform_alignments_net_variants(self, net, im, fm, var_list=None, parameters=None):
        if parameters is None:
            parameters = {}
        if var_list is None:
            variants = self.get_variants(parameters=parameters)
            var_list = [[x, y] for x,y in variants.items()]
        petri_string = pnml_exporter.export_petri_as_string(net, im, fm, parameters=parameters)
        return slave.perform_alignments(petri_string, var_list, parameters=parameters)

    def perform_tbr_net_log(self, net, im, fm, log, parameters=None):
        if parameters is None:
            parameters = {}
        variants = log_variants_filter.get_variants_from_log_trace_idx(log, parameters=parameters)
        var_list = [[x, y] for x,y in variants.items()]

        result = self.perform_tbr_net_variants(net, im, fm, var_list=var_list, parameters=parameters)

        al_idx = {}
        for index_variant, variant in enumerate(variants):
            for trace_idx in variants[variant]:
                al_idx[trace_idx] = result[index_variant]

        tbr = []
        for i in range(len(log)):
            tbr.append(al_idx[i])

        return tbr

    def perform_tbr_net_variants(self, net, im, fm, var_list=None, parameters=None):
        if parameters is None:
            parameters = {}
        if var_list is None:
            variants = self.get_variants(parameters=parameters)
            var_list = [[x, y] for x,y in variants.items()]
        petri_string = pnml_exporter.export_petri_as_string(net, im, fm, parameters=parameters)
        return slave.perform_token_replay(petri_string, var_list, parameters=parameters)

def apply(path, parameters=None):
    if parameters is None:
        parameters = {}

    return ClassicDistrLogObject(path, parameters=parameters)
