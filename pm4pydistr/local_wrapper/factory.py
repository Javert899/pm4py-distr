from pm4pydistr.local_wrapper.versions import classic

CLASSIC = "classic"

VERSIONS = {CLASSIC: classic.apply}


def apply(path, variant=CLASSIC, parameters=None):
    return VERSIONS[variant](path, parameters=parameters)
