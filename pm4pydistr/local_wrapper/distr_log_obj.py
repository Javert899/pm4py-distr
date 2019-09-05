class DistrLogObj(object):
    def __init__(self, distr_log_path, parameters=None):
        if parameters is None:
            parameters = {}
        self.distr_log_path = distr_log_path
        self.init_parameters = parameters
        self.filters = []

    def add_filter(self, filter_name, filter_value):
        raise Exception("not implemented")

    def remove_filter(self, filter_name, filter_value):
        raise Exception("not implemented")

    def calculate_dfg(self, parameters=None):
        raise Exception("not implemented")
