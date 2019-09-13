import uuid

class DistrLogObj(object):
    def __init__(self, hostname, port, keyphrase, log_name, parameters=None):
        if parameters is None:
            parameters = {}
        self.hostname = hostname
        self.port = port
        self.keyphrase = keyphrase
        self.log_name = log_name
        self.init_parameters = parameters
        self.session = str(uuid.uuid4())
        self.filters = []

    def add_filter(self, filter_name, filter_value):
        raise Exception("not implemented")

    def reset_filters(self):
        raise Exception("not implemented")

    def calculate_dfg(self, parameters=None):
        raise Exception("not implemented")

    def get_end_activities(self):
        raise Exception("not implemented")

    def get_start_activities(self):
        raise Exception("not implemented")

    def do_log_assignment(self):
        raise Exception("not implemented")
