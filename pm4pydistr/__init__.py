try:
    import pm4pycvxopt
except:
    pass

from pm4pydistr import log_handlers, master, slave, util, configuration, local_wrapper, remote_wrapper

__version__ = '0.1.18'
__doc__ = "Support for distributed logs and computations in PM4Py"
__author__ = 'PADS'
__author_email__ = 'pm4py@pads.rwth-aachen.de'
__maintainer__ = 'PADS'
__maintainer_email__ = "pm4py@pads.rwth-aachen.de"
