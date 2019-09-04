from pm4pydistr.local_wrapper.distr_log_obj import DistrLogObj


class ClassicDistrLogObject(DistrLogObj):
    def __init__(self, distr_log_path, parameters=None):
        DistrLogObj.__init__(self, distr_log_path, parameters=parameters)


def apply(path, parameters=None):
    if parameters is None:
        parameters = {}

    return ClassicDistrLogObject(path, parameters=parameters)
