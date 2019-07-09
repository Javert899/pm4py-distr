from pm4pydistr.configuration import PARAMETERS_PORT, PARAMETERS_HOST, PARAMETERS_MASTER_HOST, PARAMETERS_MASTER_PORT, \
    PARAMETERS_CONF

from pm4pydistr.slave.slave_service import SlaveSocketListener

import os

class Slave:
    def __init__(self, parameters):
        self.parameters = parameters
        self.host = parameters[PARAMETERS_HOST]
        self.port = str(parameters[PARAMETERS_PORT])
        self.master_host = parameters[PARAMETERS_MASTER_HOST]
        self.master_port = str(parameters[PARAMETERS_MASTER_PORT])
        self.conf = parameters[PARAMETERS_CONF]

        if not os.path.exists(self.conf):
            os.mkdir(self.conf)

        self.service = SlaveSocketListener(self.host, self.port, self.master_host, self.master_port, self.conf)
        self.service.run()

