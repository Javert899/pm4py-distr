from pm4pydistr.local_wrapper.versions import classic
from pm4pydistr.local_wrapper.distr_log_obj import LocalDistrLogObj

CLASSIC = "classic"

VERSIONS = {CLASSIC: classic.apply}


def apply(path, variant=CLASSIC, parameters=None) -> LocalDistrLogObj:
    return VERSIONS[variant](path, parameters=parameters)
