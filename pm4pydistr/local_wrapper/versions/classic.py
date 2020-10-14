from pm4pydistr.local_wrapper.distr_log_obj import LocalDistrLogObj
from pm4pydistr.log_handlers import parquet as parquet_handler
from pm4pydistr.util.parquet_importer import importer as parquet_importer
from pm4py.util import constants as pm4py_constants
from pathlib import Path
from pm4py.algo.filtering.common.attributes import attributes_common
from pm4py.statistics.traces.common import case_duration as case_duration_commons
from datetime import datetime
from pm4py.objects.petri.exporter.variants import pnml as pnml_exporter
from pm4py.algo.filtering.log.variants import variants_filter as log_variants_filter
from pm4pydistr.slave import slave
from pm4py.algo.discovery.causal import algorithm as causal_discovery
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.discovery.inductive.variants.im_d import dfg_based
from pm4py.objects.conversion.process_tree import converter
import numpy as np
import json


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
        lp = parquet_importer.get_list_parquet(self.distr_log_path)

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

    def get_events_per_time_first(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters

        ret = parquet_handler.get_events_per_time_first(".", self.distr_log_path, list_logs, parameters=parameters)
        ret = [datetime.fromtimestamp(x) for x in ret]

        x, y = attributes_common.get_kde_date_attribute(ret)

        return x, y

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

    def events_per_case(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()

        return parquet_handler.get_events_per_case(".", self.distr_log_path, list_logs, parameters=parameters)


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
        align = slave.perform_alignments(petri_string, var_list, parameters=parameters)
        return align

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

    def get_distr_log_footprints(self, parameters=None):
        comp_obj = self.calculate_composite_object(parameters=parameters)

        parallel = {(x, y) for (x, y) in comp_obj["frequency_dfg"] if (y, x) in comp_obj["frequency_dfg"]}
        sequence = set(causal_discovery.apply(comp_obj["frequency_dfg"], causal_discovery.Variants.CAUSAL_ALPHA))

        ret = {}
        ret["dfg"] = comp_obj["frequency_dfg"]
        ret["sequence"] = sequence
        ret["parallel"] = parallel
        ret["start_activities"] = set(comp_obj["start_activities"])
        ret["end_activities"] = set(comp_obj["end_activities"])

        return ret

    def get_imd_tree_from_dfg(self, parameters=None):
        comp_obj = self.calculate_composite_object(parameters=parameters)
        tree = dfg_based.apply_tree_dfg(comp_obj["frequency_dfg"], start_activities=comp_obj["start_activities"],
                                        end_activities=comp_obj["end_activities"], activities=comp_obj["activities"])
        return tree

    def get_imd_net_im_fm_from_dfg(self, parameters=None):
        tree = self.get_imd_tree_from_dfg(parameters=parameters)
        net, im, fm = converter.apply(tree, parameters=parameters)
        return net, im, fm

    def get_im_tree_from_variants(self, parameters=None):
        variants = {x["variant"]: x["count"] for x in self.get_variants(parameters=parameters)["variants"]}
        tree = inductive_miner.apply_tree_variants(variants, parameters=parameters)
        return tree

    def get_im_net_im_fm_from_variants(self, parameters=None):
        tree = self.get_im_tree_from_variants(parameters=parameters)
        net, im, fm = converter.apply(tree, parameters=parameters)
        return net, im, fm

    def correlation_miner(self, parameters=None):
        if parameters is None:
            parameters = {}
        list_logs = self.get_list_logs()
        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]
        parameters["filters"] = self.filters
        activities_counter = self.get_attribute_values("concept:name", parameters=parameters)
        activities = list(activities_counter.keys())
        parameters["activities"] = activities

        ret = parquet_handler.correlation_miner(".", self.distr_log_path, list_logs, parameters=parameters)
        PS_matrix = np.asmatrix(json.loads(ret["PS_matrix"]))
        duration_matrix = np.asmatrix(json.loads(ret["duration_matrix"]))

        from pm4py.algo.discovery.correlation_mining.variants import classic

        dfg, performance_dfg = classic.resolve_lp_get_dfg(PS_matrix, duration_matrix, activities, activities_counter)

        return dfg, performance_dfg


def apply(path, parameters=None):
    if parameters is None:
        parameters = {}

    return ClassicDistrLogObject(path, parameters=parameters)
