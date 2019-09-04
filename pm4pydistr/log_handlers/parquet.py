import os
from pm4py.objects.log.importer.parquet import factory as parquet_importer
from collections import Counter
from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics
from pm4py.algo.filtering.pandas.end_activities import end_activities_filter
from pm4py.algo.filtering.pandas.start_activities import start_activities_filter
from pm4py.algo.filtering.common.filtering_constants import CASE_CONCEPT_NAME
from pm4py.objects.log.util.xes import DEFAULT_NAME_KEY
from pm4pydistr.log_handlers.parquet_filtering import factory as parquet_filtering_factory

from pathlib import Path

FILTERS = "filters"

def calculate_dfg(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    filters = parameters[FILTERS] if FILTERS in parameters else []

    folder = os.path.join(path, log_name)

    parquet_list = parquet_importer.get_list_parquet(folder)
    overall_dfg = Counter()
    for pq in parquet_list:
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            df = parquet_importer.apply(pq, parameters={"columns": [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY]})
            df = parquet_filtering_factory.apply_filters(df, filters, parameters=parameters)
            dfg = Counter(df_statistics.get_dfg_graph(df, sort_timestamp_along_case_id=False, sort_caseid_required=False))
            overall_dfg = overall_dfg + dfg
    returned_dict = {}
    for el in overall_dfg:
        returned_dict[el[0] + "@@" + el[1]] = overall_dfg[el]
    return returned_dict


def get_end_activities(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    filters = parameters[FILTERS] if FILTERS in parameters else []

    folder = os.path.join(path, log_name)

    parquet_list = parquet_importer.get_list_parquet(folder)
    overall_ea = Counter()
    for pq in parquet_list:
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            df = parquet_importer.apply(pq, parameters={"columns": [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY]})
            df = parquet_filtering_factory.apply_filters(df, filters, parameters=parameters)
            ea = Counter(end_activities_filter.get_end_activities(df))
            overall_ea = overall_ea + ea

    for el in overall_ea:
        overall_ea[el] = int(overall_ea[el])

    return dict(overall_ea)


def get_start_activities(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    filters = parameters[FILTERS] if FILTERS in parameters else []

    folder = os.path.join(path, log_name)

    parquet_list = parquet_importer.get_list_parquet(folder)
    overall_sa = Counter()
    for pq in parquet_list:
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            df = parquet_importer.apply(pq, parameters={"columns": [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY]})
            df = parquet_filtering_factory.apply_filters(df, filters, parameters=parameters)
            ea = Counter(start_activities_filter.get_start_activities(df))
            overall_sa = overall_sa + ea

    for el in overall_sa:
        overall_sa[el] = int(overall_sa[el])

    return dict(overall_sa)
