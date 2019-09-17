class LocalDistrLogObj(object):
    def __init__(self, distr_log_path, parameters=None):
        if parameters is None:
            parameters = {}
        self.distr_log_path = distr_log_path
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
