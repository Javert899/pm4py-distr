from pm4pydistr.local_wrapper.distr_log_obj import DistrLogObj
from pm4pydistr.log_handlers import parquet as parquet_handler
from pm4py.objects.log.importer.parquet.versions import pyarrow

class ClassicDistrLogObject(DistrLogObj):
    def __init__(self, distr_log_path, parameters=None):
        DistrLogObj.__init__(self, distr_log_path, parameters=parameters)

    def get_list_logs(self):
        return pyarrow.apply(self.distr_log_path)

    def calculate_dfg(self, parameters=None):
        list_logs = self.get_list_logs()
        return parquet_handler.calculate_dfg(".", self.distr_log_path, list_logs, parameters=parameters)


def apply(path, parameters=None):
    if parameters is None:
        parameters = {}

    return ClassicDistrLogObject(path, parameters=parameters)
