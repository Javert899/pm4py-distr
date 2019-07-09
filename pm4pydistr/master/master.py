from pm4pydistr.configuration import PARAMETERS_PORT

from pm4pydistr.master.master_service import MasterSocketListener


class Master:
    def __init__(self, parameters):
        self.parameters = parameters
        self.slaves = {}
        self.service = MasterSocketListener(str(self.parameters[PARAMETERS_PORT]), self)
        self.service.run()


