from pm4pydistr.master.master_service import MasterSocketListener
from pm4pydistr.master.variable_container import MasterVariableContainer

from pm4pydistr.configuration import PARAMETERS_PORT, PARAMETERS_HOST, PARAMETERS_CONF, BASE_FOLDER_LIST_OPTIONS
from pm4pydistr.util.parquet_importer import importer as parquet_importer
from pm4pydistr.master.rqsts.master_assign_request import MasterAssignRequest
from pm4pydistr.master.rqsts.dfg_calc_request import DfgCalcRequest
from pm4pydistr.master.rqsts.ea_request import EaRequest
from pm4pydistr.master.rqsts.sa_request import SaRequest
from pm4pydistr.master.rqsts.filter_request import FilterRequest
from pm4pydistr.master.rqsts.attr_names_req import AttributesNamesRequest
from pm4pydistr.master.rqsts.log_summ_request import LogSummaryRequest
from pm4pydistr.master.rqsts.attr_values_req import AttrValuesRequest
from pm4pydistr.master.rqsts.perf_dfg_calc_request import PerfDfgCalcRequest
from pm4pydistr.master.rqsts.comp_obj_calc_request import CompObjCalcRequest
from pm4pydistr.master.rqsts.variants import VariantsRequest
from pm4pydistr.master.rqsts.cases_list import CasesListRequest
from pm4pydistr.master.rqsts.events import EventsRequest
from pm4pydistr.master.rqsts.events_dotted_request import EventsDottedRequest
from pm4pydistr.master.rqsts.events_per_case_requests import EventsPerCaseRequest
from pm4pydistr.master.rqsts.events_per_time_request import EventsPerTimeRequest
from pm4pydistr.master.rqsts.events_per_time_first_request import EventsPerTimeFirstRequest
from pm4pydistr.master.rqsts.case_duration_request import CaseDurationRequest
from pm4pydistr.master.rqsts.numeric_attribute_request import NumericAttributeRequest
from pm4pydistr.master.rqsts.caching_request import CachingRequest
from pm4pydistr.master.rqsts.conf_align_request import AlignRequest
from pm4pydistr.master.rqsts.conf_tbr_request import TbrRequest
from pm4pydistr.master.rqsts.shutdown_request import ShutdownRequest
from pm4pydistr.master.rqsts.corr_mining_req import CorrMiningRequest
import math
import uuid
import json

from pathlib import Path
from random import randrange
import os
import numpy as np
from collections import Counter
from pm4pydistr.master.session_checker import SessionChecker
from pm4pydistr.configuration import DEFAULT_WINDOW_SIZE
from pm4py.util import points_subset
from pm4py.objects.log.util import xes
import time
import sys


class Master:
    def __init__(self, parameters):
        self.parameters = parameters

        self.unique_identifier = str(uuid.uuid4())

        self.host = parameters[PARAMETERS_HOST]
        self.port = str(parameters[PARAMETERS_PORT])
        self.conf = parameters[PARAMETERS_CONF]
        self.base_folders = BASE_FOLDER_LIST_OPTIONS

        self.sublogs_id = {}
        self.sublogs_correspondence = {}

        self.service = MasterSocketListener(self, self.port, self.conf)
        self.service.start()

        MasterVariableContainer.dbmanager.create_log_db()
        self.load_logs()

        self.slaves = {}

        self.session_checker = SessionChecker(self)
        self.session_checker.start()

        # wait that the master really comes up
        time.sleep(0.5)

        MasterVariableContainer.master_initialization_done = True

    def load_logs(self):
        all_logs = MasterVariableContainer.dbmanager.get_logs_from_db()

        for basepath in self.base_folders:
            for folder in os.listdir(basepath):
                if folder not in self.sublogs_id:
                    self.sublogs_id[folder] = {}
                    cpath = os.path.join(basepath, folder)
                    all_parquets = parquet_importer.get_list_parquet(cpath)
                    all_parquets_basepath = [Path(x).name for x in all_parquets]

                    for name in all_parquets_basepath:
                        if name in all_logs:
                            id = all_logs[name]
                        else:
                            id = [randrange(0, 10), randrange(0, 10), randrange(0, 10), randrange(0, 10),
                                  randrange(0, 10),
                                  randrange(0, 10), randrange(0, 10)]
                            MasterVariableContainer.dbmanager.insert_log_into_db(name, id)
                        self.sublogs_id[folder][name] = id

        MasterVariableContainer.first_loading_done = True

    def do_assignment(self):
        if not MasterVariableContainer.log_assignment_done:
            all_slaves = list([eval(x) for x in self.slaves.keys()])
            for slave in all_slaves:
                self.sublogs_correspondence[str(slave)] = {}

            for folder in self.sublogs_id:
                all_logs = list(self.sublogs_id[folder])

                for slave in all_slaves:
                    self.sublogs_correspondence[str(slave)][folder] = []

                for log in all_logs:
                    distances = sorted(
                        [(x, np.linalg.norm(np.array(x) - np.array(self.sublogs_id[folder][log])), self.slaves[str(x)])
                         for x in all_slaves], key=lambda x: (x[1], x[2]))

                    self.sublogs_correspondence[str(distances[0][0])][folder].append(log)

            MasterVariableContainer.log_assignment_done = True

    def make_slaves_load(self):
        if not MasterVariableContainer.slave_loading_requested:
            all_slaves = list(self.slaves.keys())

            i = 0
            while i < len(MasterVariableContainer.assign_request_threads):
                t = MasterVariableContainer.assign_request_threads[i]
                if t.slave_finished == 1:
                    del MasterVariableContainer.assign_request_threads[i]
                    continue
                i = i + 1

            for slave in all_slaves:
                slave_host = self.slaves[slave][1]
                slave_port = str(self.slaves[slave][2])

                dictio = {"logs": self.sublogs_correspondence[slave]}

                m = MasterAssignRequest(None, slave_host, slave_port, False, 100000, dictio)
                m.start()

                MasterVariableContainer.assign_request_threads.append(m)

            MasterVariableContainer.slave_loading_requested = True

    def set_filter(self, session, process, data, use_transition, no_samples):
        all_slaves = list(self.slaves.keys())

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = FilterRequest(session, slave_host, slave_port, use_transition, no_samples,
                              {"process": process, "data": data})
            m.start()

    def calculate_dfg(self, session, process, use_transition, no_samples, attribute_key):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = DfgCalcRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.attribute_key = attribute_key
            m.start()

            threads.append(m)

        overall_dfg = Counter()

        for thread in threads:
            thread.join()

            overall_dfg = overall_dfg + Counter(thread.content['dfg'])

        return overall_dfg

    def calculate_performance_dfg(self, session, process, use_transition, no_samples, attribute_key):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = PerfDfgCalcRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.attribute_key = attribute_key
            m.start()

            threads.append(m)

        overall_dfg = Counter()

        for thread in threads:
            thread.join()

            overall_dfg = overall_dfg + Counter(thread.content['dfg'])

        for el in overall_dfg:
            overall_dfg[el] = overall_dfg[el] / len(threads)

        return overall_dfg

    def calculate_composite_obj(self, session, process, use_transition, no_samples, attribute_key,
                                performance_required=False):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = CompObjCalcRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.attribute_key = attribute_key
            m.performance_required = performance_required
            m.start()

            threads.append(m)

        overall_obj = {}
        overall_obj["events"] = 0
        overall_obj["cases"] = 0
        overall_obj["activities"] = Counter()
        overall_obj["start_activities"] = Counter()
        overall_obj["end_activities"] = Counter()
        overall_obj["frequency_dfg"] = Counter()
        if performance_required:
            overall_obj["performance_dfg"] = Counter()

        for thread in threads:
            thread.join()

            overall_obj["events"] = overall_obj["events"] + thread.content['obj']["events"]
            overall_obj["cases"] = overall_obj["cases"] + thread.content['obj']["cases"]
            overall_obj["activities"] = overall_obj["activities"] + Counter(thread.content['obj']["activities"])
            overall_obj["start_activities"] = overall_obj["start_activities"] + Counter(
                thread.content['obj']["start_activities"])
            overall_obj["end_activities"] = overall_obj["end_activities"] + Counter(
                thread.content['obj']["end_activities"])
            overall_obj["frequency_dfg"] = overall_obj["frequency_dfg"] + Counter(
                thread.content['obj']["frequency_dfg"])
            if performance_required:
                overall_obj["performance_dfg"] = overall_obj["performance_dfg"] + Counter(
                    thread.content['obj']["performance_dfg"])

        overall_obj["activities"] = dict(overall_obj["activities"])
        overall_obj["start_activities"] = dict(overall_obj["start_activities"])
        overall_obj["end_activities"] = dict(overall_obj["end_activities"])
        overall_obj["frequency_dfg"] = dict(overall_obj["frequency_dfg"])
        if performance_required:
            for el in overall_obj["performance_dfg"]:
                overall_obj["performance_dfg"][el] = overall_obj["performance_dfg"][el] / len(threads)
            overall_obj["performance_dfg"] = dict(overall_obj["performance_dfg"])

        return overall_obj

    def get_end_activities(self, session, process, use_transition, no_samples):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = EaRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.start()

            threads.append(m)

        overall_ea = Counter()

        for thread in threads:
            thread.join()

            overall_ea = overall_ea + Counter(thread.content['end_activities'])

        return overall_ea

    def get_start_activities(self, session, process, use_transition, no_samples):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = SaRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.start()

            threads.append(m)

        overall_sa = Counter()

        for thread in threads:
            thread.join()

            overall_sa = overall_sa + Counter(thread.content['start_activities'])

        return overall_sa

    def get_attribute_values(self, session, process, use_transition, no_samples, attribute_key):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = AttrValuesRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.attribute_key = attribute_key
            m.start()

            threads.append(m)

        values = Counter()

        for thread in threads:
            thread.join()

            values = values + Counter(thread.content['values'])

        return values

    def get_attributes_names(self, session, process, use_transition, no_samples):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = AttributesNamesRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.start()

            threads.append(m)

        names = set()

        for thread in threads:
            thread.join()

            names = names.union(set(thread.content['names']))

        return sorted(list(names))

    def get_log_summary(self, session, process, use_transition, no_samples):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = LogSummaryRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.start()

            threads.append(m)

        ret = {"events": 0, "cases": 0}

        for thread in threads:
            thread.join()

            ret["events"] = ret["events"] + thread.content["summary"]['events']
            ret["cases"] = ret["cases"] + thread.content["summary"]['cases']

        return ret

    def get_variants(self, session, process, use_transition, no_samples, start=0, window_size=DEFAULT_WINDOW_SIZE):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = VariantsRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.window_size = window_size
            m.start_parameter = start
            m.start()

            threads.append(m)

        dictio_variants = {}
        events = 0
        cases = 0

        for thread in threads:
            thread.join()

            d_variants = {x["variant"]: x for x in thread.content["variants"]}
            events = events + thread.content["events"]
            cases = cases + thread.content["cases"]

            for variant in d_variants:
                if not variant in dictio_variants:
                    dictio_variants[variant] = d_variants[variant]
                else:
                    dictio_variants[variant]["count"] = dictio_variants[variant]["count"] + d_variants[variant]["count"]

            list_variants = sorted(list(dictio_variants.values()), key=lambda x: x["count"], reverse=True)
            list_variants = list_variants[:min(len(list_variants), window_size)]
            dictio_variants = {x["variant"]: x for x in list_variants}

        list_variants = sorted(list(dictio_variants.values()), key=lambda x: x["count"], reverse=True)

        return {"variants": list_variants, "events": events, "cases": cases}

    def get_cases(self, session, process, use_transition, no_samples, start=0, window_size=DEFAULT_WINDOW_SIZE):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = CasesListRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.window_size = window_size
            m.start_parameter = start
            m.start()

            threads.append(m)

        cases_list = []
        events = 0
        cases = 0

        for thread in threads:
            thread.join()

            c_list = thread.content["cases_list"]

            cases_list = sorted(cases_list + c_list, key=lambda x: x["caseDuration"], reverse=True)
            cases_list = cases_list[start:min(len(cases_list), window_size)]

            events = events + thread.content["events"]
            cases = cases + thread.content["cases"]

        return {"cases_list": cases_list, "events": events, "cases": cases}

    def get_events(self, session, process, use_transition, no_samples, case_id):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = EventsRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.case_id = case_id
            m.start()

            threads.append(m)

        events = []

        for thread in threads:
            thread.join()

            ev = thread.content["events"]
            if ev:
                events = ev

        return events

    def get_events_per_dotted(self, session, process, use_transition, no_samples, attribute1, attribute2, attribute3,
                              max_ret_items=10000):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = EventsDottedRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.max_ret_items = max_ret_items
            m.attribute1 = attribute1
            m.attribute2 = attribute2
            m.attribute3 = attribute3

            m.start()

            threads.append(m)

            break

        for thread in threads:
            thread.join()

            return thread.content

    def get_events_per_case(self, session, process, use_transition, no_samples, max_ret_items=100000):
        all_slaves = list(self.slaves.keys())

        threads = []

        ret = {}

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = EventsPerCaseRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.max_ret_items = max_ret_items

            m.start()

            threads.append(m)

        for thread in threads:
            thread.join()

            d = thread.content["events_case"]

            for k in d:
                if not k in ret:
                    ret[k] = 0
                ret[k] = ret[k] + d[k]

        return ret

    def get_events_per_time(self, session, process, use_transition, no_samples, max_ret_items=100000,
                            timestamp_key=xes.DEFAULT_TIMESTAMP_KEY):
        all_slaves = list(self.slaves.keys())

        threads = []
        points = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = EventsPerTimeRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.max_ret_items = max_ret_items
            m.timestamp_key = timestamp_key

            m.start()

            threads.append(m)

        for thread in threads:
            thread.join()

            points = points + thread.content["points"]

        points = sorted(points)
        if len(points) > max_ret_items:
            points = points_subset.pick_chosen_points_list(max_ret_items, points)

        return points

    def get_events_per_time_first(self, session, process, use_transition, no_samples, max_ret_items=100000):
        all_slaves = list(self.slaves.keys())

        threads = []
        points = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = EventsPerTimeFirstRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.max_ret_items = max_ret_items

            m.start()

            threads.append(m)

        for thread in threads:
            thread.join()

            points = points + thread.content["points"]

        points = sorted(points)
        if len(points) > max_ret_items:
            points = points_subset.pick_chosen_points_list(max_ret_items, points)

        return points

    def get_case_duration(self, session, process, use_transition, no_samples, max_ret_items=100000):
        all_slaves = list(self.slaves.keys())

        threads = []
        points = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = CaseDurationRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.max_ret_items = max_ret_items

            m.start()

            threads.append(m)

        for thread in threads:
            thread.join()

            points = points + thread.content["points"]

        points = sorted(points)
        if len(points) > max_ret_items:
            points = points_subset.pick_chosen_points_list(max_ret_items, points)

        return points

    def get_numeric_attribute_values(self, session, process, use_transition, no_samples, attribute_key,
                                     max_ret_items=100000):
        all_slaves = list(self.slaves.keys())

        threads = []
        points = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = NumericAttributeRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.max_ret_items = max_ret_items
            m.attribute_key = attribute_key

            m.start()

            threads.append(m)

        for thread in threads:
            thread.join()

            points = points + thread.content["points"]

        points = sorted(points)
        if len(points) > max_ret_items:
            points = points_subset.pick_chosen_points_list(max_ret_items, points)

        return points

    def do_caching(self, session, process, use_transition, no_samples):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = CachingRequest(session, slave_host, slave_port, use_transition, no_samples, process)

            m.start()

            threads.append(m)

        for thread in threads:
            thread.join()

        return None

    def chunks(self, l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def perform_alignments(self, session, process, use_transition, no_samples, petri_string, var_list,
                           max_align_time=sys.maxsize, max_align_time_trace=sys.maxsize,
                           align_variant="dijkstra_less_memory", classic_alignments_variant="dijkstra_less_memory",
                           tree_align_variant="matrix_lp", petri_conversion_version="to_petri_net",
                           require_ilp_computation="False", max_thread_join_time=sys.maxsize):
        all_slaves = list(self.slaves.keys())

        n = math.ceil(len(var_list) / len(all_slaves))
        variants_list_split = list(self.chunks(var_list, n))

        threads = []

        for index, slave in enumerate(all_slaves):
            if len(variants_list_split) > index:
                slave_host = self.slaves[slave][1]
                slave_port = str(self.slaves[slave][2])
                var_list = variants_list_split[index]

                var_list = sorted(var_list, key=lambda x: len(x[0].split(',')))

                content = {"petri_string": petri_string, "var_list": var_list,
                           "max_align_time": max_align_time, "max_align_time_trace": max_align_time_trace,
                           "align_variant": align_variant, "classic_alignments_variant": classic_alignments_variant,
                           "tree_align_variant": tree_align_variant, "petri_conversion_version": petri_conversion_version,
                           "require_ilp_computation": require_ilp_computation}

                m = AlignRequest(session, slave_host, slave_port, use_transition, no_samples, process, content)

                m.start()

                threads.append(m)

        ret_dict = {}
        rem_waiting_time = 0 + max_thread_join_time

        for index, thread in enumerate(threads):
            try:
                # max align time
                aa = time.time()
                if rem_waiting_time < 4000000:
                    thread.join(rem_waiting_time)
                else:
                    thread.join()
                bb = time.time()
                rem_waiting_time = max(0.001, rem_waiting_time - (bb - aa))

                try:
                    ret_dict.update(thread.content["alignments"])
                except:
                    import traceback
                    raise Exception(thread.content)
            except:
                import traceback
                #traceback.print_exc()
                for var in variants_list_split[index]:
                    ret_dict[var[0]] = None

        return ret_dict

    def perform_tbr(self, session, process, use_transition, no_samples, petri_string, var_list,
                    enable_parameters_precision, consider_remaining_in_fitness):
        all_slaves = list(self.slaves.keys())

        n = math.ceil(len(var_list) / len(all_slaves))
        variants_list_split = list(self.chunks(var_list, n))

        threads = []

        for index, slave in enumerate(all_slaves):
            if len(variants_list_split) > index:
                slave_host = self.slaves[slave][1]
                slave_port = str(self.slaves[slave][2])

                content = {"petri_string": petri_string, "var_list": variants_list_split[index],
                           "enable_parameters_precision": enable_parameters_precision,
                           "consider_remaining_in_fitness": consider_remaining_in_fitness}

                m = TbrRequest(session, slave_host, slave_port, use_transition, no_samples, process, content)

                m.start()

                threads.append(m)

        ret_dict = []

        for thread in threads:
            thread.join()

            ret_dict = ret_dict + thread.content["tbr"]

        return ret_dict

    def perform_shutdown(self, session, process, use_transition, no_samples):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = ShutdownRequest(session, slave_host, slave_port, use_transition, no_samples, None)
            m.start()

            threads.append(m)

        # do shutdown
        os._exit(0)

        for thread in threads:
            thread.join()

        return None

    def correlation_miner(self, session, process, use_transition, no_samples, activities, start_timestamp,
                          complete_timestamp):
        all_slaves = list(self.slaves.keys())

        threads = []
        PS_matrixes = []
        duration_matrixes = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            content = {"activities": activities, "start_timestamp": start_timestamp,
                       "complete_timestamp": complete_timestamp}

            m = CorrMiningRequest(session, slave_host, slave_port, use_transition, no_samples, process, content)
            m.start()

            threads.append(m)

        for thread in threads:
            thread.join()

            PS_matrixes.append(thread.PS_matrix)
            duration_matrixes.append(thread.duration_matrix)

        PS_matrix = np.zeros((len(activities), len(activities)))
        duration_matrix = np.zeros((len(activities), len(activities)))

        z = 0
        while z < len(PS_matrixes):
            try:
                PS_matrix = PS_matrix + np.asmatrix(PS_matrixes[z]).reshape(len(activities), len(activities))
                duration_matrix = np.maximum(duration_matrix, np.asmatrix(duration_matrixes[z]).reshape(len(activities), len(activities)))
            except:
                pass
            z = z + 1

        PS_matrix = PS_matrix / float(len(PS_matrixes))

        PS_matrix = PS_matrix.tolist()
        duration_matrix = duration_matrix.tolist()

        return {"PS_matrix": json.dumps(PS_matrix), "duration_matrix": json.dumps(duration_matrix)}
