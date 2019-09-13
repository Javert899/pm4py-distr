from pm4pydistr.local_wrapper.versions import classic
from pm4pydistr.local_wrapper.distr_log_obj import DistrLogObj

CLASSIC = "classic"

VERSIONS = {CLASSIC: classic.apply}


def apply(path, variant=CLASSIC, parameters=None) -> DistrLogObj:
    return VERSIONS[variant](path, parameters=parameters)
