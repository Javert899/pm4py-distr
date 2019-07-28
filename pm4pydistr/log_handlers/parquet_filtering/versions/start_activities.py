from pm4py.algo.filtering.pandas.start_activities import start_activities_filter


def apply(dataframe, filter, parameters=None):
    """
    Apply a filter to the current log (start activities filter)

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

    return start_activities_filter.apply(dataframe, filter[1], parameters=parameters)
