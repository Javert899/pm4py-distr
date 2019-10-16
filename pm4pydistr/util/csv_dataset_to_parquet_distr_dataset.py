from pm4py.objects.log.adapters.pandas import csv_import_adapter
from pm4py.objects.log.exporter.parquet import factory as parquet_exporter
from pm4py.objects.log.importer.parquet import factory as parquet_importer
import os
import pandas as pd
from pm4py.objects.log.util import xes
from pm4py.algo.filtering.common.filtering_constants import CASE_CONCEPT_NAME


def transform_csv_dataset_to_parquet_distr_dataset(source_path, target_path, target_num_partitions, activity_key=xes.DEFAULT_NAME_KEY, timestamp_key=xes.DEFAULT_TIMESTAMP_KEY, caseid_key=CASE_CONCEPT_NAME):
    """
    Transforms the CSV dataset to a Parquet distributed dataset

    Parameters
    -------------
    source_path
        Source path
    target_path
        Target path
    target_num_partitions
        Target number of partitions (number of divisions of the output)
    activity_key
        Column that is the activity
    timestamp_key
        Column that is the timestamp
    caseid_key
        Column that is the case ID

    Returns
    -------------
    void
    """
    # create the folder
    try:
        os.mkdir(target_path)
    except:
        pass

    # create the partitions
    dataframe = pd.DataFrame({})
    for i in range(target_num_partitions):
        tp = os.path.join(target_path, str(i)+".parquet")
        parquet_exporter.apply(dataframe, tp)
    files = os.listdir(source_path)
    for index, file in enumerate(files):
        if file.lower().endswith("csv"):
            sp = os.path.join(source_path, file)
            source_df = csv_import_adapter.import_dataframe_from_path(sp)
            if activity_key != xes.DEFAULT_NAME_KEY:
                source_df[xes.DEFAULT_NAME_KEY] = source_df[activity_key]
            if timestamp_key != xes.DEFAULT_TIMESTAMP_KEY:
                source_df[xes.DEFAULT_TIMESTAMP_KEY] = source_df[timestamp_key]
            if caseid_key != CASE_CONCEPT_NAME:
                source_df[CASE_CONCEPT_NAME] = source_df[caseid_key]
            source_df["@@partition"] = source_df[caseid_key].rank(method='dense', ascending=False).astype(int) % target_num_partitions
            for i in range(target_num_partitions):
                print("input %d/%d output %d/%d" % (index+1,len(files),i+1,target_num_partitions))
                tp = os.path.join(target_path, str(i)+".parquet")
                df2 = source_df[source_df["@@partition"] == i]
                del df2["@@partition"]
                #df2 = df2.reset_index()
                df1 = parquet_importer.apply(tp)
                df = pd.concat([df1, df2])
                if index == len(files)-1:
                    df = df.sort_values([caseid_key, timestamp_key])
                parquet_exporter.apply(df, tp)
