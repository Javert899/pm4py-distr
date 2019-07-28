from pm4py.algo.filtering.pandas.cases import case_filter
from pm4py.util import constants as pm4_constants
from pm4py.objects.log.util import xes


def apply(dataframe, filter, parameters=None):
    """
    Applies a filter on case performance

    Parameters
    --------------
    dataframe
        Dataframe
    filter
        Filter (two performance bounds separated by @@@)
    parameters
        Parameters of the algorithm

    Returns
    ---------------
    filtered_df
        Filtered dataframe
    """
    if parameters is None:
        parameters = {}

    case_id_key = parameters[
        pm4_constants.PARAMETER_CONSTANT_CASEID_KEY] if pm4_constants.PARAMETER_CONSTANT_CASEID_KEY in parameters else "case:concept:name"
    timestamp_key = parameters[
        pm4_constants.PARAMETER_CONSTANT_TIMESTAMP_KEY] if pm4_constants.PARAMETER_CONSTANT_TIMESTAMP_KEY in parameters else xes.DEFAULT_TIMESTAMP_KEY

    min_case_performance = float(filter[1].split("@@@")[0])
    max_case_performance = float(filter[1].split("@@@")[1])

    return case_filter.filter_on_case_performance(dataframe, case_id_glue=case_id_key, timestamp_key=timestamp_key,
                                                  min_case_performance=min_case_performance,
                                                  max_case_performance=max_case_performance)
