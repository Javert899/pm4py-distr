from pm4py.algo.filtering.pandas.ltl import ltl_checker


def apply(dataframe, filter, parameters=None):
    """
    Apply an LTL checker filter on the dataframe

    Parameters
    --------------
    dataframe
        Dataframe
    filter
        Content of the filter (dictionary including the type, the arguments and the parameters)

    Returns
    -------------
    filtered_dataframe
        Filtered dataframe
    """
    if parameters is None:
        parameters = {}

    filter = filter[1]

    args = filter["arguments"]

    for param in filter["parameters"]:
        parameters[param] = filter["parameters"][param]

    if filter["type"] == "A_eventually_B":
        return ltl_checker.A_eventually_B(dataframe, args[0], args[1], parameters=parameters)
    elif filter["type"] == "A_eventually_B_eventually_C":
        return ltl_checker.A_eventually_B_eventually_C(dataframe, args[0], args[1], args[2],
                                                       parameters=parameters)
    elif filter["type"] == "A_eventually_B_eventually_C_eventually_D":
        return ltl_checker.A_eventually_B_eventually_C_eventually_D(dataframe, args[0], args[1], args[2], args[3],
                                                                    parameters=parameters)
    elif filter["type"] == "A_next_B_next_C":
        return ltl_checker.A_next_B_next_C(dataframe, args[0], args[1], args[2],
                                                       parameters=parameters)
    elif filter["type"] == "four_eyes_principle":
        return ltl_checker.four_eyes_principle(dataframe, args[0], args[1],
                                                       parameters=parameters)
    elif filter["type"] == "attr_value_different_persons":
        return ltl_checker.four_eyes_principle(dataframe, args[0],
                                                       parameters=parameters)

    return dataframe
