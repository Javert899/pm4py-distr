from pm4pydistr.master.master_service import MasterSocketListener
from pm4pydistr.master.variable_container import MasterVariableContainer

from pm4pydistr.configuration import PARAMETERS_PORT, PARAMETERS_HOST, PARAMETERS_CONF, BASE_FOLDER_LIST_OPTIONS
from pm4py.objects.log.importer.parquet import factory as parquet_importer
from pathlib import Path
from random import randrange
import os
import numpy as np

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

                distances = sorted([(x, np.linalg.norm(np.array(x) - np.array(self.sublogs_id[folder][log]))) for x in all_slaves], key=lambda x: x[1])

                self.sublogs_correspondence[str(distances[0][0])][folder].append(log)
