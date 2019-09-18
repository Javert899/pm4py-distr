import os
from pm4py.objects.log.importer.parquet import factory as parquet_importer
from collections import Counter
from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics
from pm4py.algo.filtering.pandas.end_activities import end_activities_filter
from pm4py.algo.filtering.pandas.start_activities import start_activities_filter
from pm4py.algo.filtering.common.filtering_constants import CASE_CONCEPT_NAME
from pm4py.objects.log.util.xes import DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY, DEFAULT_TRANSITION_KEY
from pm4py.util import constants as pm4py_constants
from pm4pydistr.configuration import PARAMETER_USE_TRANSITION, DEFAULT_USE_TRANSITION
from pm4pydistr.configuration import PARAMETER_NO_SAMPLES, DEFAULT_MAX_NO_SAMPLES
from pm4pydistr.log_handlers.parquet_filtering import factory as parquet_filtering_factory
import pyarrow.parquet as pqq


from pathlib import Path

FILTERS = "filters"
ALLOWED_FILTERS = {"end_activities", "start_activities", "variants"}


def get_columns_to_import(filters, columns, use_transition=False):
    columns = set(columns)

    if filters:
        fkeys = set(f[0] for f in filters)
        if "start_activities" in fkeys or "end_activities" in fkeys or "variants" in fkeys:
            columns.add(DEFAULT_NAME_KEY)
        if "timestamp_events" in fkeys or "timestamp_trace_containing" in fkeys or "timestamp_trace_intersecting" in fkeys or "timestamp_trace_intersecting" in fkeys:
            columns.add(DEFAULT_TIMESTAMP_KEY)
        for f in filters:
            if type(f[1]) is list:
                columns.add(f[0])
    if use_transition:
        columns.add(DEFAULT_NAME_KEY)
        columns.add(DEFAULT_TRANSITION_KEY)
    columns = list(columns)
    return columns

def insert_classifier(df):
    df["@@classifier"] = df[DEFAULT_NAME_KEY] + "+" + df[DEFAULT_TRANSITION_KEY]
    return df

def calculate_dfg(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else "@@classifier"
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY], use_transition=use_transition)

    if pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY in parameters:
        columns.append(parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY])
        activity_key, parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY], activity_key
    else:
        parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = activity_key
    folder = os.path.join(path, log_name)

    parquet_list = parquet_importer.get_list_parquet(folder)
    overall_dfg = Counter()
    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1
            df = parquet_importer.apply(pq, parameters={"columns": columns})

            if use_transition:
                df = insert_classifier(df)
            if filters:
                df = parquet_filtering_factory.apply_filters(df, filters, parameters=parameters)
            dfg = Counter(
                df_statistics.get_dfg_graph(df, activity_key=activity_key, sort_timestamp_along_case_id=False, sort_caseid_required=False))
            overall_dfg = overall_dfg + dfg
            if count >= no_samples:
                break

    returned_dict = {}
    for el in overall_dfg:
        returned_dict[el[0] + "@@" + el[1]] = overall_dfg[el]
    return returned_dict

def get_end_activities(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else "@@classifier"
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = activity_key

    folder = os.path.join(path, log_name)
    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY], use_transition=use_transition)

    parquet_list = parquet_importer.get_list_parquet(folder)
    overall_ea = Counter()
    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1
            df = parquet_importer.apply(pq, parameters={"columns": columns})

            if use_transition:
                df = insert_classifier(df)
            if filters:
                df = parquet_filtering_factory.apply_filters(df, filters, parameters=parameters)
            ea = Counter(end_activities_filter.get_end_activities(df, parameters=parameters))
            overall_ea = overall_ea + ea
            if count >= no_samples:
                break

    for el in overall_ea:
        overall_ea[el] = int(overall_ea[el])

    return dict(overall_ea)


def get_start_activities(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else "@@classifier"
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = activity_key

    folder = os.path.join(path, log_name)
    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY], use_transition=use_transition)

    parquet_list = parquet_importer.get_list_parquet(folder)
    overall_sa = Counter()
    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1
            df = parquet_importer.apply(pq, parameters={"columns": columns})

            if use_transition:
                df = insert_classifier(df)
            if filters:
                df = parquet_filtering_factory.apply_filters(df, filters, parameters=parameters)

            ea = Counter(start_activities_filter.get_start_activities(df, parameters=parameters))
            overall_sa = overall_sa + ea
            if count >= no_samples:
                break

    for el in overall_sa:
        overall_sa[el] = int(overall_sa[el])

    return dict(overall_sa)


def get_log_summary(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else "@@classifier"
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = activity_key

    folder = os.path.join(path, log_name)
    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME], use_transition=use_transition)

    parquet_list = parquet_importer.get_list_parquet(folder)

    events = 0
    cases = 0
    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1
            df = parquet_importer.apply(pq, parameters={"columns": columns})

            if use_transition and filters:
                df = insert_classifier(df)
            if filters:
                df = parquet_filtering_factory.apply_filters(df, filters, parameters=parameters)

            events = events + len(df)
            cases = cases + df[CASE_CONCEPT_NAME].nunique()
            if count >= no_samples:
                break

    return {"events": events, "cases": cases}


def get_attribute_values(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else "@@classifier"
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key

    attribute_key = parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] if pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY in parameters else DEFAULT_NAME_KEY

    folder = os.path.join(path, log_name)
    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, attribute_key], use_transition=use_transition)

    parquet_list = parquet_importer.get_list_parquet(folder)
    dictio = Counter({})

    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1
            df = parquet_importer.apply(pq, parameters={"columns": columns})

            if use_transition and filters:
                df = insert_classifier(df)
            if filters:
                df = parquet_filtering_factory.apply_filters(df, filters, parameters=parameters)

            dictio = dictio + Counter(dict(df[attribute_key].value_counts()))
            if count >= no_samples:
                break

    dictio = dict(dictio)
    for el in dictio:
        dictio[el] = int(dictio[el])

    return dictio


def get_attribute_names(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    folder = os.path.join(path, log_name)

    parquet_list = parquet_importer.get_list_parquet(folder)
    names = set()

    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1
            names = names.union(set(pqq.read_metadata(pq).schema.names))
            if count >= no_samples:
                break

    names = sorted(list(names))
    names = [x.replace("AAA",":") for x in names]

    return sorted(list(names))
