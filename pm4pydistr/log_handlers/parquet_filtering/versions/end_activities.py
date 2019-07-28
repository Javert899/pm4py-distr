from pm4py.algo.filtering.pandas.end_activities import end_activities_filter


def apply(dataframe, filter, parameters=None):
    """
    Apply a filter to the current log (end activities filter)

    Parameters
    ------------
    dataframe
        Pandas dataframe
    filter
        Filter to apply
    parameters
        Parameters of the algorithm

    Returns
    ------------
    dataframe
        Pandas dataframe
    """
    if parameters is None:
        parameters = {}

    return end_activities_filter.apply(dataframe, filter[1], parameters=parameters)
