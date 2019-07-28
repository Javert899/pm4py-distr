from pm4py.algo.filtering.pandas.variants import variants_filter


def apply(dataframe, filter, parameters=None):
    """
    Apply a filter to the current log (variants filter)

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

    return variants_filter.apply(dataframe, filter[1], parameters=parameters)
