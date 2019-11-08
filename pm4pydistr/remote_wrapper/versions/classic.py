from pm4pydistr.remote_wrapper.distr_log_obj import DistrLogObj
from pm4py.algo.filtering.common.attributes import attributes_common
from pm4py.statistics.traces.common import case_duration as case_duration_commons
from pm4pydistr.configuration import PARAMETER_USE_TRANSITION, DEFAULT_USE_TRANSITION
from pm4pydistr.configuration import PARAMETER_NO_SAMPLES, DEFAULT_MAX_NO_SAMPLES
from pm4pydistr.configuration import PARAMETER_NUM_RET_ITEMS
import requests
import json
import time
from pm4py.util import constants
from pm4py.objects.petri import align_utils
from pm4py.algo.conformance.alignments.versions import state_equation_a_star
from datetime import datetime
from pm4py.objects.petri.exporter.versions import pnml as pnml_exporter
from pm4py.algo.filtering.log.variants import variants_filter as log_variants_filter
from pm4pydistr.slave import slave
import sys

PARAM_MAX_ALIGN_TIME_TRACE = "max_align_time_trace"
DEFAULT_MAX_ALIGN_TIME_TRACE = sys.maxsize
PARAM_MAX_ALIGN_TIME = "max_align_time"
DEFAULT_MAX_ALIGN_TIME = sys.maxsize


class ClassicDistrLogObject(DistrLogObj):
    def __init__(self, hostname, port, keyphrase, log_name, parameters=None):
        DistrLogObj.__init__(self, hostname, port, keyphrase, log_name, parameters=parameters)
        self.check_connession()

    def check_connession(self):
        url = self.get_url("getLoadingStatus")
        max_retry_conn = 13
        sleep_time = 0.1
        connected = False
        for i in range(max_retry_conn):
            try:
                r = requests.get(url)
                content = json.loads(r.text)
                if content["keyphrase_correct"]:
                    if content["first_loading_done"]:
                        if not content["log_assignment_done"]:
                            url2 = self.get_url("doLogAssignment")
                            print(time.time(), "doing initial log assignment into slaves")
                            r2 = requests.get(url2)
                            print(time.time(), "done initial log assignment into slaves (they are working now :) )")
                        connected = True
                    else:
                        print(time.time(), "services still loading")
                else:
                    print(time.time(), "password uncorrect!")
                    break
            except:
                print(time.time(), "connection with host failed (%d out of %d)" % (i + 1, max_retry_conn))
                if i + 1 == max_retry_conn:
                    break
                sleep_time = sleep_time * 1.5
                time.sleep(sleep_time)

        if not connected:
            raise Exception("impossible to set up a connection with the specified host!! launching exception")

        while True:
            r = requests.get(url)
            content = json.loads(r.text)
            if content["slave_loading_requested"] and content["finished_slaves"] > 0 and content["finished_slaves"] >= \
                    content["slaves_count"]:
                break
            else:
                print(time.time(),
                      "slaves still coming up, waiting a little more! finished log assignation for %d slaves out of %d" % (
                          content["finished_slaves"], content["slaves_count"]))
            sleep_time = sleep_time * 1.5
            time.sleep(sleep_time)

    def get_url(self, service, parameters=None):
        if parameters is None:
            parameters = {}

        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]

        use_transition = parameters[
            PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
        no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES

        stru = "http://" + self.hostname + ":" + str(
            self.port) + "/" + service + "?keyphrase=" + self.keyphrase + "&process=" + self.log_name + "&session=" + str(
            self.session) + "&use_transition=" + str(use_transition) + "&no_samples=" + str(no_samples)

        for parameter in parameters:
            if parameter == constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY:
                stru = stru + "&attribute_key=" + str(parameters[constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY])
            elif parameter == "attribute_key":
                stru = stru + "&attribute_key=" + str(parameters["attribute_key"])
            elif parameter == "performance_required":
                stru = stru + "&performance_required=" + str(parameters["performance_required"])
            elif PARAMETER_NUM_RET_ITEMS in parameters:
                stru = stru + "&" + PARAMETER_NUM_RET_ITEMS + "=" + str(parameters[PARAMETER_NUM_RET_ITEMS])
            elif parameter.startswith("attribute"):
                stru = stru + "&" + str(parameter) + "=" + str(parameters[parameter])

        return stru

    def do_log_assignment(self, parameters=None):
        url = self.get_url("doLogAssignment", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        return ret_text

    def do_caching(self, parameters=None):
        url = self.get_url("doCaching", parameters=parameters)
        r = requests.get(url)
        return None

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

    def set_filters(self, filters, parameters=None):
        self.filters = filters
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

    def calculate_performance_dfg(self, parameters=None):
        url = self.get_url("calculatePerformanceDfg", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        dfg = ret_json["dfg"]
        new_dfg = {}
        for el in dfg:
            new_dfg[(el.split("@@")[0], el.split("@@")[1])] = dfg[el]
        return new_dfg

    def calculate_composite_object(self, parameters=None):
        url = self.get_url("calculateCompositeObj", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        obj = ret_json["obj"]
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

    def get_log_summary(self, parameters=None):
        url = self.get_url("getLogSummary", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["summary"]

    def get_attribute_values(self, attribute_key, parameters=None):
        if parameters is None:
            parameters = {}
        parameters["attribute_key"] = attribute_key
        url = self.get_url("getAttributeValues", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["values"]

    def get_attribute_names(self, parameters=None):
        url = self.get_url("getAttributesNames", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["names"]

    def get_variants(self, parameters=None):
        url = self.get_url("getVariants", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json

    def get_cases(self, parameters=None):
        url = self.get_url("getCases", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json

    def get_events(self, case_id, parameters=None):
        url = self.get_url("getEvents", parameters=parameters)
        url = url + "&case_id=" + str(case_id)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["events"]

    def get_logs_list(self):
        url = self.get_url("getSublogsId")
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return sorted(list(ret_json["sublogs_id"].keys()))

    def get_events_per_dotted(self, attribute1, attribute2, attribute3, parameters=None):
        if parameters is None:
            parameters = {}
        url = self.get_url("getEventsPerDotted",
                           parameters={"attribute1": attribute1, "attribute2": attribute2, "attribute3": attribute3})
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)

        return ret_json

    def get_events_per_time(self, parameters=None):
        if parameters is None:
            parameters = {}
        url = self.get_url("getEventsPerTime")
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        ret = ret_json["points"]
        ret = [datetime.fromtimestamp(x) for x in ret]
        x, y = attributes_common.get_kde_date_attribute(ret)

        return x, y

    def get_case_duration(self, parameters=None):
        if parameters is None:
            parameters = {}
        url = self.get_url("getCaseDuration")
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        ret = ret_json["points"]

        x, y = case_duration_commons.get_kde_caseduration(ret)

        return x, y

    def get_numeric_attribute(self, attribute_key, parameters=None):
        if parameters is None:
            parameters = {}
        url = self.get_url("getNumericAttributeValues", parameters={"attribute_key": attribute_key})
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        ret = ret_json["points"]

        x, y = attributes_common.get_kde_numeric_attribute(ret)

        return x, y

    def perform_alignments_net_log(self, net, im, fm, log, parameters=None):
        if parameters is None:
            parameters = {}
        variants = log_variants_filter.get_variants_from_log_trace_idx(log, parameters=parameters)
        var_list = [[x, y] for x, y in variants.items()]

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
            var_list = [[x["variant"], x["count"]] for x in variants["variants"]]
        petri_string = pnml_exporter.export_petri_as_string(net, im, fm, parameters=parameters)
        return self.perform_alignments(petri_string, var_list, parameters=parameters)

    def perform_alignments(self, petri_string, var_list, parameters=None):
        if parameters is None:
            parameters = {}

        max_align_time = parameters[
            PARAM_MAX_ALIGN_TIME] if PARAM_MAX_ALIGN_TIME in parameters else DEFAULT_MAX_ALIGN_TIME
        max_align_time_trace = parameters[
            PARAM_MAX_ALIGN_TIME_TRACE] if PARAM_MAX_ALIGN_TIME_TRACE in parameters else DEFAULT_MAX_ALIGN_TIME_TRACE

        url = self.get_url("performAlignments", parameters=parameters)
        dictio = {"petri_string": petri_string, "var_list": var_list, "max_align_time": max_align_time,
                  "max_align_time_trace": max_align_time_trace}

        r = requests.post(url, json=dictio)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["alignments"]

    def perform_tbr_net_log(self, net, im, fm, log, parameters=None):
        if parameters is None:
            parameters = {}
        variants = log_variants_filter.get_variants_from_log_trace_idx(log, parameters=parameters)
        var_list = [[x, y] for x, y in variants.items()]

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
            var_list = [[x["variant"], x["count"]] for x in variants["variants"]]
        petri_string = pnml_exporter.export_petri_as_string(net, im, fm, parameters=parameters)
        return self.perform_token_replay(petri_string, var_list, parameters=parameters)

    def perform_token_replay(self, petri_string, var_list, parameters=None):
        if parameters is None:
            parameters = {}

        enable_parameters_precision = parameters["enable_parameters_precision"] if "enable_parameters_precision" in parameters else False

        url = self.get_url("performTbr")
        dictio = {"petri_string": petri_string, "var_list": var_list, "enable_parameters_precision": enable_parameters_precision}

        r = requests.post(url, json=dictio)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["tbr"]

    def calculate_fitness_with_tbr(self, net, im, fm, log, parameters=None):
        if parameters is None:
            parameters = {}
        variants = log_variants_filter.get_variants_from_log_trace_idx(log, parameters=parameters)
        var_list = [[x, y] for x, y in variants.items()]

        result = self.perform_tbr_net_variants(net, im, fm, var_list=var_list, parameters=parameters)
        total_cases = 0
        total_fit_cases = 0
        sum_of_fitness = 0
        total_m = 0
        total_r = 0
        total_c = 0
        total_p = 0

        for index_variant, variant in enumerate(variants):
            if result[index_variant] is not None:
                sum_of_fitness = sum_of_fitness + len(variants[variant]) * result[index_variant]["trace_fitness"]
                total_m = total_m + len(variants[variant]) * result[index_variant]["missing_tokens"]
                total_r = total_r + len(variants[variant]) * result[index_variant]["remaining_tokens"]
                total_c = total_c + len(variants[variant]) * result[index_variant]["consumed_tokens"]
                total_p = total_p + len(variants[variant]) * result[index_variant]["produced_tokens"]

                total_cases = total_cases + len(variants[variant])
                if result[index_variant]["trace_is_fit"]:
                    total_fit_cases = total_fit_cases + len(variants[variant])

        if total_cases > 0:
            perc_fit_traces = float(100.0 * total_fit_cases) / float(total_cases)
            average_fitness = float(sum_of_fitness) / float(total_cases)
            log_fitness = 0.5 * (1 - total_m / total_c) + 0.5 * (1 - total_r / total_p)

            return {"perc_fit_traces": perc_fit_traces, "average_trace_fitness": average_fitness, "log_fitness": log_fitness}

        return {"perc_fit_traces": 0.0, "average_trace_fitness": 0.0, "log_fitness": 0.0}


    def fitness_alignment_internal(self, best_worst_cost, trace, cost):
        len_trace = len(trace.split(","))

        unfitness_upper_part = cost // align_utils.STD_MODEL_LOG_MOVE_COST
        fitness = 0
        if unfitness_upper_part == 0:
            fitness = 1
        elif (len_trace + best_worst_cost) > 0:
            fitness = 1 - (
                    (cost // align_utils.STD_MODEL_LOG_MOVE_COST) / (
                    len_trace + best_worst_cost))

        return fitness

    def calculate_fitness_with_alignments(self, net, im, fm, log, parameters=None):
        if parameters is None:
            parameters = {}

        sum_fitness = 0

        variants = log_variants_filter.get_variants_from_log_trace_idx(log, parameters=parameters)
        var_list = [[x, y] for x, y in variants.items()]

        result = self.perform_alignments_net_variants(net, im, fm, var_list=var_list, parameters=parameters)
        total_cases = 0
        total_fit_cases = 0

        best_worst_cost = state_equation_a_star.get_best_worst_cost(net, im, fm, parameters={})

        for index_variant, variant in enumerate(variants):
            total_cases = total_cases + len(variants[variant])
            if result[variant] is not None:
                fitness = self.fitness_alignment_internal(best_worst_cost, variant, result[variant]["cost"])
                sum_fitness = sum_fitness + fitness * len(variants[variant])

                if result[variant]["cost"] < 10000:
                    total_fit_cases = total_fit_cases + len(variants[variant])

        if total_cases > 0:
            perc_fit_traces = float(100.0 * total_fit_cases) / float(total_cases)

            return {"averageFitness": float(sum_fitness)/float(total_cases), "percFitTraces": perc_fit_traces}
        return {"averageFitness": 0.0, "percFitTraces": 0.0}

    def calculate_precision_with_tbr(self, net, im, fm, log, parameters=None):
        from pm4py import util as pmutil
        from pm4py.algo.conformance.tokenreplay import factory as token_replay
        from pm4py.objects import log as log_lib
        from pm4py.evaluation.precision import utils as precision_utils

        if parameters is None:
            parameters = {}

        sum_at = 0.0
        sum_ee = 0.0

        prefixes, prefix_count = precision_utils.get_log_prefixes(log)
        print("got prefixes")
        prefixes_keys = list(prefixes.keys())
        fake_log = precision_utils.form_fake_log(prefixes_keys)
        print("got fake log")

        variants = log_variants_filter.get_variants_from_log_trace_idx(fake_log, parameters=parameters)
        print("got variants from fake log")
        var_list = [[x, y] for x, y in variants.items()]
        print("got var list")

        parameters["enable_parameters_precision"] = True
        aligned_traces = self.perform_tbr_net_variants(net, im, fm, var_list=var_list, parameters=parameters)
        print("got aligned traces")

        for i in range(len(aligned_traces)):
            if aligned_traces[i]["trace_is_fit"]:
                log_transitions = set(prefixes[prefixes_keys[i]])
                activated_transitions_labels = set(
                    [x for x in aligned_traces[i]["enabled_transitions_in_marking_labels"] if x != "None"])
                sum_at += len(activated_transitions_labels) * prefix_count[prefixes_keys[i]]
                escaping_edges = activated_transitions_labels.difference(log_transitions)
                sum_ee += len(escaping_edges) * prefix_count[prefixes_keys[i]]

        if sum_at > 0:
            precision = 1 - float(sum_ee) / float(sum_at)

        return precision
