import os
from pm4py.objects.log.importer.parquet import factory as parquet_importer
from collections import Counter
from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics
from pm4py.algo.filtering.common.filtering_constants import CASE_CONCEPT_NAME
from pm4py.objects.log.util.xes import DEFAULT_NAME_KEY
from pathlib import Path

def calculate_dfg(path, log_name, managed_logs, parameters=None):
    if parameters is None:
        parameters = {}

    folder = os.path.join(path, log_name)

    print(managed_logs)

    parquet_list = parquet_importer.get_list_parquet(folder)
    overall_dfg = Counter()
    for pq in parquet_list:
        pq_basename = Path(pq).name
        if pq_basename in managed_logs:
            df = parquet_importer.apply(pq, parameters={"columns": [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY]})
            dfg = Counter(df_statistics.get_dfg_graph(df, sort_timestamp_along_case_id=False, sort_caseid_required=False))
            overall_dfg = overall_dfg + dfg
    returned_dict = {}
    for el in overall_dfg:
        returned_dict[el[0] + "@@" + el[1]] = overall_dfg[el]
    return returned_dict
