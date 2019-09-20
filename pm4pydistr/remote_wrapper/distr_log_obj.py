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

    def add_filter(self, filter_name, filter_value, parameters=None):
        raise Exception("not implemented")

    def reset_filters(self, parameters=None):
        raise Exception("not implemented")

    def calculate_dfg(self, parameters=None):
        raise Exception("not implemented")

    def calculate_performance_dfg(self, parameters=None):
        raise Exception("not implemented")

    def calculate_composite_object(self, parameters=None):
        raise Exception("not implemented")

    def get_end_activities(self, parameters=None):
        raise Exception("not implemented")

    def get_start_activities(self, parameters=None):
        raise Exception("not implemented")

    def do_log_assignment(self, parameters=None):
        raise Exception("not implemented")

    def get_log_summary(self, parameters=None):
        raise Exception("not implemented")

    def get_attribute_values(self, attribute_key, parameters=None):
        raise Exception("not implemented")

    def get_attribute_names(self, parameters=None):
        raise Exception("not implemented")

    def get_variants(self, parameters=None):
        raise Exception("not implemented")

    def get_cases(self, parameters=None):
        raise Exception("not implemented")

    def get_events(self, case_id, parameters=None):
        raise Exception("not implemented")

    def get_logs_list(self):
        raise Exception("not implemented")
