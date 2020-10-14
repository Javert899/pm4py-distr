import os
from pm4pydistr.util.parquet_importer import importer as parquet_importer
from collections import Counter
from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics
from pm4py.algo.filtering.pandas.end_activities import end_activities_filter
from pm4py.algo.filtering.pandas.start_activities import start_activities_filter
from pm4py.algo.filtering.common.filtering_constants import CASE_CONCEPT_NAME
from pm4py.objects.log.util.xes import DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY, DEFAULT_TRANSITION_KEY
from pm4py.util import constants as pm4py_constants
from pm4pydistr.configuration import PARAMETER_USE_TRANSITION, DEFAULT_USE_TRANSITION
from pm4pydistr.configuration import PARAMETER_NO_SAMPLES, DEFAULT_MAX_NO_SAMPLES
from pm4pydistr.configuration import PARAMETER_NUM_RET_ITEMS, DEFAULT_WINDOW_SIZE, PARAMETER_START, \
    PARAMETER_WINDOW_SIZE, PARAMETER_PM4PYWS_CLASSIFIER
from pm4pydistr.log_handlers.parquet_filtering import factory as parquet_filtering_factory
import pyarrow.parquet as pqq
from pm4py.statistics.traces.pandas import case_statistics
import pandas as pd
import numpy as np
from pm4py.util import points_subset
import json

from pathlib import Path

PARQUET_CACHE = {}
FILTERS = "filters"


def get_columns_to_import(filters, columns, use_transition=False):
    if filters is None:
        filters = []
    if columns is None:
        columns = []
    columns = set(columns)

    if filters:
        fkeys = set(f[0] for f in filters)
        if "start_activities" in fkeys or "end_activities" in fkeys or "variants" in fkeys:
            columns.add(DEFAULT_NAME_KEY)
            columns.add(CASE_CONCEPT_NAME)
        if "timestamp_events" in fkeys or "timestamp_trace_containing" in fkeys or "timestamp_trace_intersecting" in fkeys or "timestamp_trace_intersecting" in fkeys or "case_performance_filter" in fkeys:
            columns.add(DEFAULT_TIMESTAMP_KEY)
            columns.add(CASE_CONCEPT_NAME)
        if "paths_pos_trace" in fkeys or "paths_neg_trace" in fkeys:
            columns.add(DEFAULT_TIMESTAMP_KEY)
            columns.add(CASE_CONCEPT_NAME)
        if "ltl" in fkeys:
            columns.add(DEFAULT_TIMESTAMP_KEY)
            columns.add(CASE_CONCEPT_NAME)
            columns.add(DEFAULT_NAME_KEY)
        for f in filters:
            if type(f[1]) is list:
                columns.add(f[1][0])
            if "cas" in f[0] or "trac" in f[0] or "var" in f[0]:
                columns.add(CASE_CONCEPT_NAME)
                columns.add(DEFAULT_NAME_KEY)
    if use_transition:
        columns.add(DEFAULT_NAME_KEY)
        columns.add(DEFAULT_TRANSITION_KEY)
    columns = list(columns)
    return columns


def insert_classifier(df):
    df[PARAMETER_PM4PYWS_CLASSIFIER] = df[DEFAULT_NAME_KEY] + "+" + df[DEFAULT_TRANSITION_KEY]
    return df


def load_parquet_from_path(path, columns, filters, use_transition=False, force_classifier_insertion=False,
                           force_timestamp_conversion=False, parameters=None):
    if parameters is None:
        parameters = {}
    if filters is None:
        filters = []
    if columns is None:
        columns = []
        df = parquet_importer.apply(path)
    else:
        df = parquet_importer.apply(path, parameters={"columns": columns})

    if DEFAULT_TIMESTAMP_KEY in columns and (filters or force_timestamp_conversion):
        df[DEFAULT_TIMESTAMP_KEY] = pd.to_datetime(df[DEFAULT_TIMESTAMP_KEY], utc=True)

    if use_transition:
        df = insert_classifier(df)
    elif force_classifier_insertion:
        df[PARAMETER_PM4PYWS_CLASSIFIER] = df[DEFAULT_NAME_KEY]

    return df


def get_filtered_parquet(path, columns, filters, use_transition=False, force_classifier_insertion=False,
                         parameters=None):
    if parameters is None:
        parameters = {}
    if filters is None:
        filters = []
    if columns is None:
        columns = []
    if path in PARQUET_CACHE and not use_transition and set(columns).issubset(
            set([CASE_CONCEPT_NAME, DEFAULT_TIMESTAMP_KEY, DEFAULT_NAME_KEY])):
        df = PARQUET_CACHE[path]
    else:
        df = load_parquet_from_path(path, columns, filters, use_transition=use_transition,
                                    force_classifier_insertion=force_classifier_insertion, parameters=parameters)

    if filters:
        df = parquet_filtering_factory.apply_filters(df, filters, parameters=parameters)

    return df


def do_caching(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    folder = os.path.join(path, log_name)
    parquet_list = parquet_importer.get_list_parquet(folder)
    count = 0

    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1

            df = load_parquet_from_path(pq, [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY], [],
                                        use_transition=False, force_classifier_insertion=True,
                                        force_timestamp_conversion=True)
            PARQUET_CACHE[pq] = df

            if count >= no_samples:
                break


def calculate_dfg(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY], use_transition=use_transition)

    if pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY in parameters:
        columns.append(parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY])
        activity_key, parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = parameters[
                                                                                         pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY], activity_key
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
            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)
            dfg = Counter(
                df_statistics.get_dfg_graph(df, activity_key=activity_key, sort_timestamp_along_case_id=False,
                                            sort_caseid_required=False))
            overall_dfg = overall_dfg + dfg
            if count >= no_samples:
                break

    returned_dict = {}
    for el in overall_dfg:
        returned_dict[el[0] + "@@" + el[1]] = overall_dfg[el]
    return returned_dict


def calculate_performance_dfg(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY],
                                    use_transition=use_transition)

    if pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY in parameters:
        columns.append(parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY])
        activity_key, parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = parameters[
                                                                                         pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY], activity_key
    else:
        parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = activity_key
    folder = os.path.join(path, log_name)

    parquet_list = parquet_importer.get_list_parquet(folder)
    frequency_dfg = Counter()
    performance_dfg = Counter()
    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1
            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)

            f_dfg, p_dfg = df_statistics.get_dfg_graph(df, activity_key=activity_key,
                                                       sort_timestamp_along_case_id=False, sort_caseid_required=False,
                                                       measure="both")
            f_dfg = Counter(f_dfg)

            for k in p_dfg:
                if k not in performance_dfg:
                    performance_dfg[k] = p_dfg[k]
                else:
                    performance_dfg[k] = (frequency_dfg[k] * performance_dfg[k] + f_dfg[k] * p_dfg[k]) / (
                                frequency_dfg[k] + f_dfg[k])

            frequency_dfg = frequency_dfg + f_dfg
            if count >= no_samples:
                break

    returned_dict = {}
    for el in performance_dfg:
        returned_dict[el[0] + "@@" + el[1]] = performance_dfg[el]
    return returned_dict


def calculate_process_schema_composite_object(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    performance_required = parameters["performance_required"] if "performance_required" in parameters else False
    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    if performance_required:
        columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY],
                                        use_transition=use_transition)
    else:
        columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY], use_transition=use_transition)

    if pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY in parameters:
        columns.append(parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY])
        activity_key, parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = parameters[
                                                                                         pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY], activity_key
    else:
        parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = activity_key
    folder = os.path.join(path, log_name)

    parquet_list = parquet_importer.get_list_parquet(folder)
    frequency_dfg = Counter()
    performance_dfg = Counter()
    overall_ea = Counter()
    overall_sa = Counter()
    values = Counter({})
    events = 0
    cases = 0

    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1
            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)

            if performance_required:
                f_dfg, p_dfg = df_statistics.get_dfg_graph(df, activity_key=activity_key,
                                                           sort_timestamp_along_case_id=False,
                                                           sort_caseid_required=False, measure="both")
            else:
                f_dfg = df_statistics.get_dfg_graph(df, activity_key=activity_key, sort_timestamp_along_case_id=False,
                                                    sort_caseid_required=False)

            f_dfg = Counter(f_dfg)

            if performance_required:
                for k in p_dfg:
                    if k not in performance_dfg:
                        performance_dfg[k] = p_dfg[k]
                    else:
                        performance_dfg[k] = (frequency_dfg[k] * performance_dfg[k] + f_dfg[k] * p_dfg[k]) / (
                                frequency_dfg[k] + f_dfg[k])

            frequency_dfg = frequency_dfg + f_dfg
            ea = Counter(end_activities_filter.get_end_activities(df, parameters=parameters))
            overall_ea = overall_ea + ea
            sa = Counter(start_activities_filter.get_start_activities(df, parameters=parameters))
            overall_sa = overall_sa + sa
            values = values + Counter(dict(df[activity_key].value_counts()))
            events = events + len(df)
            cases = cases + df[CASE_CONCEPT_NAME].nunique()

            if count >= no_samples:
                break

    returned_dict = {}
    returned_dict["events"] = events
    returned_dict["cases"] = cases
    values = dict(values)
    for el in values:
        values[el] = int(values[el])
    returned_dict["activities"] = values
    overall_sa = dict(overall_sa)
    for el in overall_sa:
        overall_sa[el] = int(overall_sa[el])
    returned_dict["start_activities"] = overall_sa
    overall_ea = dict(overall_ea)
    for el in overall_ea:
        overall_ea[el] = int(overall_ea[el])
    returned_dict["end_activities"] = overall_ea
    returned_dict_freq = {}
    for el in frequency_dfg:
        returned_dict_freq[el[0] + "@@" + el[1]] = int(frequency_dfg[el])
    returned_dict["frequency_dfg"] = returned_dict_freq
    if performance_required:
        returned_dict_perf = {}
        for el in performance_dfg:
            returned_dict_perf[el[0] + "@@" + el[1]] = float(performance_dfg[el])
        returned_dict["performance_dfg"] = returned_dict_perf

    return returned_dict


def get_end_activities(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
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
            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)

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
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
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
            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)

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
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
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
            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)

            events = events + len(df)
            cases = cases + df[CASE_CONCEPT_NAME].nunique()
            if count >= no_samples:
                break

    return {"events": events, "cases": cases}


def get_attribute_values(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key

    attribute_key = parameters[
        pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] if pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY in parameters else DEFAULT_NAME_KEY

    folder = os.path.join(path, log_name)
    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, attribute_key], use_transition=use_transition)

    parquet_list = parquet_importer.get_list_parquet(folder)
    dictio = Counter({})

    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1
            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)

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
    names = [x.replace("AAA", ":") for x in names]

    return sorted(list(names))


def get_variants(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    window_size = parameters[PARAMETER_NUM_RET_ITEMS] if PARAMETER_NUM_RET_ITEMS in parameters else DEFAULT_WINDOW_SIZE
    start = parameters[PARAMETER_START] if PARAMETER_START in parameters else 0
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key

    folder = os.path.join(path, log_name)
    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY],
                                    use_transition=use_transition)

    parquet_list = parquet_importer.get_list_parquet(folder)

    dictio_variants = {}
    events = 0
    cases = 0

    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1
            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters,
                                      force_classifier_insertion=True)

            events = events + len(df)
            cases = cases + df[CASE_CONCEPT_NAME].nunique()

            # dictio = dictio + Counter(dict(df[attribute_key].value_counts()))
            stats = case_statistics.get_variant_statistics(df)
            d_variants = {x["variant"]: x for x in stats}

            for variant in d_variants:
                d_variants[variant]["count"] = d_variants[variant]["case:concept:name"]
                del d_variants[variant]["case:concept:name"]
                if not variant in dictio_variants:
                    dictio_variants[variant] = d_variants[variant]
                else:
                    dictio_variants[variant]["count"] = dictio_variants[variant]["count"] + d_variants[variant]["count"]

            list_variants = sorted(list(dictio_variants.values()), key=lambda x: x["count"], reverse=True)
            list_variants = list_variants[start:min(len(list_variants), window_size)]
            dictio_variants = {x["variant"]: x for x in list_variants}

            if count >= no_samples:
                break

    list_variants = sorted(list(dictio_variants.values()), key=lambda x: x["count"], reverse=True)

    return {"variants": list_variants, "events": events, "cases": cases}


def get_cases(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    window_size = parameters[PARAMETER_NUM_RET_ITEMS] if PARAMETER_NUM_RET_ITEMS in parameters else DEFAULT_WINDOW_SIZE
    start = parameters[PARAMETER_START] if PARAMETER_START in parameters else 0
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key

    folder = os.path.join(path, log_name)
    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY],
                                    use_transition=use_transition)

    parquet_list = parquet_importer.get_list_parquet(folder)

    cases_list = []
    events = 0
    cases = 0

    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1
            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters,
                                      force_classifier_insertion=True)

            events = events + len(df)
            cases = cases + df[CASE_CONCEPT_NAME].nunique()

            stats = case_statistics.get_cases_description(df)
            c_list = []
            for x, y in stats.items():
                c_list.append({"caseId": x, "caseDuration": y["caseDuration"], "startTime": y["startTime"],
                               "endTime": y["endTime"]})

            cases_list = sorted(cases_list + c_list, key=lambda x: x["caseDuration"], reverse=True)
            cases_list = cases_list[start:min(len(cases_list), window_size)]

            if count >= no_samples:
                break

    return {"cases_list": cases_list, "events": events, "cases": cases}


def get_events(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = activity_key

    case_id = parameters["case_id"]

    folder = os.path.join(path, log_name)

    parquet_list = parquet_importer.get_list_parquet(folder)

    ret = []

    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1

            df = get_filtered_parquet(pq, None, filters, use_transition=use_transition, parameters=parameters)

            try:
                events = case_statistics.get_events(df, case_id)
                if len(events) > 0:
                    df = parquet_importer.apply(pq)
                    ret = df[df[CASE_CONCEPT_NAME] == case_id].dropna(how="all", axis=1)
                    if activity_key != PARAMETER_PM4PYWS_CLASSIFIER:
                        ret[PARAMETER_PM4PYWS_CLASSIFIER] = ret[activity_key]
                    ret = ret.to_dict('r')
                    break
            except:
                pass

            if count >= no_samples:
                break

    return ret


def get_events_per_dotted(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = activity_key

    max_no_events = parameters["max_no_events"] if "max_no_events" in parameters else 10000

    attributes = ["@@event_index", parameters["attribute1"], parameters["attribute2"]]
    if parameters["attribute3"] is not None:
        attributes.append(parameters["attribute3"])
    attributes1 = list(set([CASE_CONCEPT_NAME] + [x for x in list(set(attributes)) if not x.startswith("@@")]))

    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, DEFAULT_TIMESTAMP_KEY] + attributes1,
                                    use_transition=use_transition)

    folder = os.path.join(path, log_name)

    parquet_list = parquet_importer.get_list_parquet(folder)

    df_list = []
    no_events = 0
    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1

            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)

            df = df[attributes1].dropna()

            no_events = no_events + len(df)

            df_list.append(df)

            if no_events >= max_no_events:
                break

            if count >= no_samples:
                break

    df = pd.concat(df_list)
    df["@@event_index"] = df.index
    df = df.sort_values([DEFAULT_TIMESTAMP_KEY, "@@event_index"])
    df = df.reset_index(drop=True)
    df["@@case_index"] = df.groupby(CASE_CONCEPT_NAME, sort=False).ngroup()
    df = df.sort_values(["@@case_index", DEFAULT_TIMESTAMP_KEY, "@@event_index"])

    stream = df.to_dict('r')
    stream = sorted(stream, key=lambda x: (x[attributes[2]], x[attributes[1]], x[attributes[0]]))
    third_unique_values = []
    if len(attributes) > 3:
        third_unique_values = sorted(list(set(s[attributes[3]] for s in stream)))
    types = {}
    if stream:
        for attr in attributes:
            val = stream[0][attr]
            types[attr] = str(type(val))
            if type(val) is pd._libs.tslibs.timestamps.Timestamp:
                for ev in stream:
                    ev[attr] = ev[attr].timestamp()
    traces = []
    if third_unique_values:
        for index, v in enumerate(third_unique_values):
            traces.append({})
            for index2, attr in enumerate(attributes):
                if index2 < len(attributes) - 1:
                    traces[-1][attr] = [s[attr] for s in stream if s[attributes[3]] == v]
    else:
        third_unique_values.append("UNIQUE")
        traces.append({})
        for index2, attr in enumerate(attributes):
            traces[-1][attr] = [s[attr] for s in stream]

    return traces, types, attributes, third_unique_values


def get_events_per_time(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
    timestamp_key = parameters["timestamp_key"] if "timestamp_key" in parameters else DEFAULT_TIMESTAMP_KEY
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = activity_key

    max_no_of_points_to_sample = parameters[
        "max_no_of_points_to_sample"] if "max_no_of_points_to_sample" in parameters else 100000

    folder = os.path.join(path, log_name)
    columns = get_columns_to_import(filters, [timestamp_key], use_transition=use_transition)

    parquet_list = parquet_importer.get_list_parquet(folder)

    overall_list = []
    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1

            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)

            if len(df) > max_no_of_points_to_sample:
                df = df.sample(n=max_no_of_points_to_sample)

            date_values = [x.timestamp() for x in list(df[timestamp_key])]
            overall_list = overall_list + date_values

            if count >= no_samples:
                break

    overall_list = sorted(overall_list)
    if len(overall_list) > max_no_of_points_to_sample:
        overall_list = points_subset.pick_chosen_points_list(max_no_of_points_to_sample, overall_list)

    return overall_list


def get_events_per_time_first(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = activity_key

    max_no_of_points_to_sample = parameters[
        "max_no_of_points_to_sample"] if "max_no_of_points_to_sample" in parameters else 100000

    folder = os.path.join(path, log_name)
    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, DEFAULT_TIMESTAMP_KEY], use_transition=use_transition)

    parquet_list = parquet_importer.get_list_parquet(folder)

    overall_list = []
    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1

            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)
            df = df.groupby(CASE_CONCEPT_NAME).first()

            if len(df) > max_no_of_points_to_sample:
                df = df.sample(n=max_no_of_points_to_sample)

            date_values = [x.timestamp() for x in list(df[DEFAULT_TIMESTAMP_KEY])]
            overall_list = overall_list + date_values

            if count >= no_samples:
                break

    overall_list = sorted(overall_list)
    if len(overall_list) > max_no_of_points_to_sample:
        overall_list = points_subset.pick_chosen_points_list(max_no_of_points_to_sample, overall_list)

    return overall_list


def get_case_duration(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = activity_key

    max_no_of_points_to_sample = parameters[
        "max_no_of_points_to_sample"] if "max_no_of_points_to_sample" in parameters else 100000

    folder = os.path.join(path, log_name)
    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME, DEFAULT_TIMESTAMP_KEY], use_transition=use_transition)

    parquet_list = parquet_importer.get_list_parquet(folder)

    overall_list = []
    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1

            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)

            cases = case_statistics.get_cases_description(df, parameters=parameters)
            duration_values = [x["caseDuration"] for x in cases.values()]

            overall_list = overall_list + duration_values

            if count >= no_samples:
                break

    overall_list = sorted(overall_list)
    if len(overall_list) > max_no_of_points_to_sample:
        overall_list = points_subset.pick_chosen_points_list(max_no_of_points_to_sample, overall_list)

    return overall_list


def get_events_per_case(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    filters = parameters[FILTERS] if FILTERS in parameters else []

    folder = os.path.join(path, log_name)
    columns = get_columns_to_import(filters, [CASE_CONCEPT_NAME], use_transition=use_transition)

    parquet_list = parquet_importer.get_list_parquet(folder)
    length_counter = Counter()

    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1

            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)

            case_dict = dict(df.groupby(CASE_CONCEPT_NAME).size())

            for case, occ in case_dict.items():
                length_counter[int(occ)] += 1

            if count >= no_samples:
                break

    return dict(length_counter)


def get_numeric_attribute_values(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = activity_key

    max_no_of_points_to_sample = parameters[
        "max_no_of_points_to_sample"] if "max_no_of_points_to_sample" in parameters else 100000

    attribute_key = parameters["attribute_key"]

    folder = os.path.join(path, log_name)
    columns = get_columns_to_import(filters, [attribute_key], use_transition=use_transition)

    parquet_list = parquet_importer.get_list_parquet(folder)

    overall_list = []
    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1

        df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)
        df = df.dropna()

        if len(df) > max_no_of_points_to_sample:
            df = df.sample(n=max_no_of_points_to_sample)

        values = list(df[attribute_key])

        overall_list = overall_list + values

        if count >= no_samples:
            break

    overall_list = sorted(overall_list)
    if len(overall_list) > max_no_of_points_to_sample:
        overall_list = points_subset.pick_chosen_points_list(max_no_of_points_to_sample, overall_list)

    return overall_list


def correlation_miner(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    activities = parameters["activities"] if "activities" in parameters else None
    complete_timestamp = parameters["complete_timestamp"] if "complete_timestamp" in parameters else DEFAULT_TIMESTAMP_KEY
    start_timestamp = parameters["start_timestamp"] if "start_timestamp" in parameters else DEFAULT_TIMESTAMP_KEY

    from pm4py.algo.discovery.correlation_mining.variants import classic

    no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES
    use_transition = parameters[
        PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
    activity_key = DEFAULT_NAME_KEY if not use_transition else PARAMETER_PM4PYWS_CLASSIFIER
    filters = parameters[FILTERS] if FILTERS in parameters else []
    parameters[pm4py_constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = activity_key
    parameters[classic.Parameters.ACTIVITY_KEY] = activity_key
    parameters[classic.Parameters.TIMESTAMP_KEY] = complete_timestamp
    parameters[classic.Parameters.START_TIMESTAMP_KEY] = start_timestamp

    folder = os.path.join(path, log_name)
    columns = get_columns_to_import(filters, [activity_key, complete_timestamp, start_timestamp], use_transition=use_transition)

    parquet_list = parquet_importer.get_list_parquet(folder)

    PS_matrixes = []
    duration_matrixes = []

    count = 0
    for index, pq in enumerate(parquet_list):
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            count = count + 1

            df = get_filtered_parquet(pq, columns, filters, use_transition=use_transition, parameters=parameters)

            transf_stream, activities_grouped, activities = classic.preprocess_log(df, activities=activities,
                                                                                   parameters=parameters)

            PS_matrix, duration_matrix = classic.get_PS_dur_matrix(activities_grouped, activities,
                                                                   parameters=parameters)

            PS_matrixes.append(PS_matrix)
            duration_matrixes.append(duration_matrix)

            if count >= no_samples:
                break

    PS_matrix = np.zeros((len(activities), len(activities)))
    duration_matrix = np.zeros((len(activities), len(activities)))

    z = 0
    while z < len(PS_matrixes):
        PS_matrix = PS_matrix + PS_matrixes[z]
        duration_matrix = np.maximum(duration_matrix, duration_matrixes[z])
        z = z + 1
    PS_matrix = PS_matrix / float(len(PS_matrixes))

    PS_matrix = PS_matrix.tolist()
    duration_matrix = duration_matrix.tolist()

    return {"PS_matrix": json.dumps(PS_matrix), "duration_matrix": json.dumps(duration_matrix)}
