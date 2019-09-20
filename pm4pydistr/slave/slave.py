from pm4pydistr.configuration import PARAMETERS_PORT, PARAMETERS_HOST, PARAMETERS_MASTER_HOST, PARAMETERS_MASTER_PORT, \
    PARAMETERS_CONF, BASE_FOLDER_LIST_OPTIONS, PARAMETERS_AUTO_HOST

from pm4pydistr.slave.slave_service import SlaveSocketListener
from pm4pydistr.slave.slave_requests import SlaveRequests
from pathlib import Path
from pm4py.objects.log.importer.parquet import factory as parquet_importer
from pm4pydistr.slave.do_ms_ping import DoMasterPing
import uuid
import socket

import os
import shutil

class Slave:
    def __init__(self, parameters):
        self.parameters = parameters
        self.host = parameters[PARAMETERS_HOST]
        self.port = str(parameters[PARAMETERS_PORT])
        self.master_host = parameters[PARAMETERS_MASTER_HOST]
        self.master_port = str(parameters[PARAMETERS_MASTER_PORT])
        self.conf = parameters[PARAMETERS_CONF]
        if PARAMETERS_AUTO_HOST in parameters and parameters[PARAMETERS_AUTO_HOST] == "1":
            #import netifaces as ni
            self.conf = str(uuid.uuid4())
            self.host = str(socket.gethostname())
            #ni.ifaddresses('eth0')
            #ip = ni.ifaddresses('eth0')[ni.AF_INET][0]['addr']
            #self.master_host = str(ip)
        self.id = None
        self.ping_module = None

        self.filters = {}

        if not os.path.exists(self.conf):
            os.mkdir(self.conf)

        self.slave_requests = SlaveRequests(self, self.host, self.port, self.master_host, self.master_port, self.conf)

        self.service = SlaveSocketListener(self, self.host, self.port, self.master_host, self.master_port, self.conf)
        self.service.start()

        self.slave_requests.register_to_webservice()

    def create_folder(self, folder_name):
        #print("create folder " + str(folder_name))
        if not os.path.isdir(os.path.join(self.conf, folder_name)):
            os.mkdir(os.path.join(self.conf, folder_name))

    def load_log(self, folder_name, log_name):
        #print("loading log " + str(log_name)+" into "+str(folder_name))
        if not os.path.exists(os.path.join(self.conf, folder_name, log_name)):
            for folder in BASE_FOLDER_LIST_OPTIONS:
                if folder_name in os.listdir(folder):
                    list_paths = parquet_importer.get_list_parquet(os.path.join(folder, folder_name))
                    list_paths_corr = {}
                    for x in list_paths:
                        list_paths_corr[Path(x).name] = x
                    if log_name in list_paths_corr:
                        #print("log_name",log_name," in ",os.path.join(folder, folder_name),list_paths_corr[log_name])
                        shutil.copyfile(list_paths_corr[log_name], os.path.join(self.conf, folder_name, log_name))

    def enable_ping_of_master(self):
        self.ping_module = DoMasterPing(self, self.conf, self.id, self.master_host, self.master_port)
        self.ping_module.start()
