from pm4py.algo.filtering.pandas.timestamp import timestamp_filter
from datetime import datetime
from copy import copy
from pm4py.util.constants import PARAMETER_CONSTANT_TIMESTAMP_KEY


def apply(dataframe, filter, parameters=None):
    """
    Apply a timestamp filter to the dataframe

    Parameters
    ------------
    dataframe
        Dataframe where the filter should be applied
    filter
        Filter (two timestamps separated by @@@)
    parameters
        Parameters of the algorithm
    """
    if parameters is None:
        parameters = {}

    dt1 = str(datetime.utcfromtimestamp(int(filter[1][1].split("@@@")[0])))
    dt2 = str(datetime.utcfromtimestamp(int(filter[1][1].split("@@@")[1])))

    new_parameters = copy(parameters)
    new_parameters[PARAMETER_CONSTANT_TIMESTAMP_KEY] = filter[1][0]

    return timestamp_filter.filter_traces_contained(dataframe, dt1, dt2, parameters=new_parameters)
