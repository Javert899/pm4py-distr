from pm4pydistr.remote_wrapper.versions import classic
from pm4pydistr.remote_wrapper.distr_log_obj import DistrLogObj

CLASSIC = "classic"

VERSIONS = {CLASSIC: classic.ClassicDistrLogObject}

def apply(hostname, port, keyphrase, log_name, variant=CLASSIC, parameters=None) -> DistrLogObj:
    if parameters is None:
        parameters = {}

    return VERSIONS[variant](hostname, port, keyphrase, log_name, parameters=parameters)
