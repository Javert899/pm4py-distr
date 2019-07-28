from pm4py.algo.filtering.pandas.paths import paths_filter
from pm4py.util import constants


def apply(dataframe, filter, parameters=None):
    """

    :param dataframe:
    :param filter:
    :param parameters:
    :return:
    """
    if parameters is None:
        parameters = {}

    parameters[constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = filter[1][0]
    parameters["positive"] = False

    paths_to_filter = []

    for p in filter[1][1]:
        paths_to_filter.append(p.split("@@"))

    return paths_filter.apply(dataframe, paths_to_filter, parameters=parameters)
