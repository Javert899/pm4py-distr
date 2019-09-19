from pm4pydistr.master.master_service import MasterSocketListener
from pm4pydistr.master.variable_container import MasterVariableContainer

from pm4pydistr.configuration import PARAMETERS_PORT, PARAMETERS_HOST, PARAMETERS_CONF, BASE_FOLDER_LIST_OPTIONS
from pm4py.objects.log.importer.parquet import factory as parquet_importer
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
from pathlib import Path
from random import randrange
import os
import numpy as np
from collections import Counter
from pm4pydistr.master.session_checker import SessionChecker
from pm4pydistr.configuration import DEFAULT_MAX_NO_RET_ITEMS


class Master:
    def __init__(self, parameters):
        self.parameters = parameters

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
                            id = [randrange(0, 10), randrange(0, 10), randrange(0, 10), randrange(0, 10), randrange(0, 10),
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

                    distances = sorted([(x, np.linalg.norm(np.array(x) - np.array(self.sublogs_id[folder][log])), self.slaves[str(x)]) for x in all_slaves], key=lambda x: (x[1], x[2]))

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

            m = FilterRequest(session, slave_host, slave_port, use_transition, no_samples, {"process": process, "data": data})
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

        return overall_dfg

    def calculate_composite_obj(self, session, process, use_transition, no_samples, attribute_key, performance_required=False):
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
            overall_obj["start_activities"] = overall_obj["start_activities"] + Counter(thread.content['obj']["start_activities"])
            overall_obj["end_activities"] = overall_obj["end_activities"] + Counter(thread.content['obj']["end_activities"])
            overall_obj["frequency_dfg"] = overall_obj["frequency_dfg"] + Counter(thread.content['obj']["frequency_dfg"])
            if performance_required:
                overall_obj["performance_dfg"] = overall_obj["performance_dfg"] + Counter(thread.content['obj']["performance_dfg"])

        overall_obj["activities"] = dict(overall_obj["activities"])
        overall_obj["start_activities"] = dict(overall_obj["start_activities"])
        overall_obj["end_activities"] = dict(overall_obj["end_activities"])
        overall_obj["frequency_dfg"] = dict(overall_obj["frequency_dfg"])
        if performance_required:
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

    def get_variants(self, session, process, use_transition, no_samples, max_ret_items=DEFAULT_MAX_NO_RET_ITEMS):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = VariantsRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.max_ret_items = max_ret_items
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
                    dictio_variants[variant]["caseDuration"] = (dictio_variants[variant]["caseDuration"] * dictio_variants[variant]["count"] + d_variants[variant]["caseDuration"] * d_variants[variant]["count"])/(dictio_variants[variant]["count"] + d_variants[variant]["count"])
                    dictio_variants[variant]["count"] = dictio_variants[variant]["count"] + d_variants[variant]["count"]

            list_variants = sorted(list(dictio_variants.values()), key=lambda x: x["count"], reverse=True)
            list_variants = list_variants[:min(len(list_variants), max_ret_items)]
            dictio_variants = {x["variant"]: x for x in list_variants}

        list_variants = sorted(list(dictio_variants.values()), key=lambda x: x["count"], reverse=True)

        return {"variants": list_variants, "events": events, "cases": cases}

    def get_cases(self, session, process, use_transition, no_samples, max_ret_items=DEFAULT_MAX_NO_RET_ITEMS):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = CasesListRequest(session, slave_host, slave_port, use_transition, no_samples, process)
            m.max_ret_items = max_ret_items
            m.start()

            threads.append(m)

        cases_list = []
        events = 0
        cases = 0

        for thread in threads:
            thread.join()

            c_list = thread.content["cases_list"]

            cases_list = sorted(cases_list + c_list, key=lambda x: x["caseDuration"], reverse=True)
            cases_list = cases_list[:min(len(cases_list), max_ret_items)]

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
