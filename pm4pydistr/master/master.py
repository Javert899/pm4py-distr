from pm4pydistr.master.master_service import MasterSocketListener
from pm4pydistr.master.variable_container import MasterVariableContainer

from pm4pydistr.configuration import PARAMETERS_PORT, PARAMETERS_HOST, PARAMETERS_CONF, BASE_FOLDER_LIST_OPTIONS
from pm4py.objects.log.importer.parquet import factory as parquet_importer
from pm4pydistr.master.rqsts.master_assign_request import MasterAssignRequest
from pm4pydistr.master.rqsts.dfg_calc_request import DfgCalcRequest
from pm4pydistr.master.rqsts.ea_request import EaRequest
from pathlib import Path
from random import randrange
import os
import numpy as np
from collections import Counter
from pm4pydistr.master.session_checker import SessionChecker

class Master:
    def __init__(self, parameters):
        self.parameters = parameters

        self.host = parameters[PARAMETERS_HOST]
        self.port = str(parameters[PARAMETERS_PORT])
        self.conf = parameters[PARAMETERS_CONF]
        self.base_folders = BASE_FOLDER_LIST_OPTIONS

        self.slaves = {}
        self.service = MasterSocketListener(self, self.port, self.conf)
        self.service.start()

        self.sublogs_id = {}
        self.sublogs_correspondence = {}

        MasterVariableContainer.dbmanager.create_log_db()
        self.load_logs()

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


    def do_assignment(self):
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


    def make_slaves_load(self):
        all_slaves = list(self.slaves.keys())

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            dictio = {"logs": self.sublogs_correspondence[slave]}

            m = MasterAssignRequest(slave_host, slave_port, dictio)
            m.start()


    def calculate_dfg(self, process):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = DfgCalcRequest(slave_host, slave_port, process)
            m.start()

            threads.append(m)

        overall_dfg = Counter()

        for thread in threads:
            thread.join()

            overall_dfg = overall_dfg + Counter(thread.content['dfg'])

        return overall_dfg


    def get_end_activities(self, process):
        all_slaves = list(self.slaves.keys())

        threads = []

        for slave in all_slaves:
            slave_host = self.slaves[slave][1]
            slave_port = str(self.slaves[slave][2])

            m = EaRequest(slave_host, slave_port, process)
            m.start()

            threads.append(m)

        overall_ea = Counter()

        for thread in threads:
            thread.join()

            overall_ea = overall_ea + Counter(thread.content['end_activities'])

        return overall_ea
