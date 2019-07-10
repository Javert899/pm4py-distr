from pm4pydistr.master.master_service import MasterSocketListener
from pm4pydistr.master.variable_container import MasterVariableContainer

from pm4pydistr.configuration import PARAMETERS_PORT, PARAMETERS_HOST, PARAMETERS_CONF


class Master:
    def __init__(self, parameters):
        self.parameters = parameters

        self.host = parameters[PARAMETERS_HOST]
        self.port = str(parameters[PARAMETERS_PORT])
        self.conf = parameters[PARAMETERS_CONF]

        self.slaves = {}
        self.service = MasterSocketListener(self, self.port, self.conf)
        self.service.start()

        MasterVariableContainer.dbmanager.create_log_db()
