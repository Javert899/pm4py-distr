class DistrLogObj(object):
    def __init__(self, distr_log_path, parameters=None):
        if parameters is None:
            parameters = {}
        self.distr_log_path = distr_log_path
        self.init_parameters = parameters

    def calculate_dfg(self, parameters=None):
        raise Exception("not implemented")
