class LocalDistrLogObj(object):
    def __init__(self, distr_log_path, parameters=None):
        if parameters is None:
            parameters = {}
        self.init_parameters = parameters
        self.distr_log_path = distr_log_path
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
