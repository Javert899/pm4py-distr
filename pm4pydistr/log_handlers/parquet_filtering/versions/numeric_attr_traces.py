from pm4py.algo.filtering.pandas.attributes import attributes_filter
from pm4py.util import constants


def apply(dataframe, filter, parameters=None):
    """
    Apply a numeric filter (traces)

    Parameters
    -------------
    dataframe
        Dataframe
    filter
        Filter to apply
    parameters
        Parameters of the algorithm

    Returns
    -------------
    dataframe
        Filtered dataframe
    """

    if parameters is None:
        parameters = {}

    parameters[constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = filter[1][0]

    min_value = float(filter[1][1].split("@@@")[0])
    max_value = float(filter[1][1].split("@@@")[1])

    return attributes_filter.apply_numeric(dataframe, min_value, max_value, parameters=parameters)
