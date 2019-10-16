import random
import numpy as np
import os

def do_random_sorting_and_partition(dataframe, no_partitions):
    """
    Random sorts the rows of a dataframe, assigning also a partition on such sorting

    Parameters
    -------------
    dataframe
        Dataframe
    no_partitions
        Number of partitions

    Returns
    -------------
    dataframe
        Dataframe with a partitioning (@@partition) column
    """
    rnum = [random.random() for i in range(len(dataframe))]
    dataf = dataframe.copy()
    dataf['random_col'] = np.random.choice(rnum, len(dataf))
    dataf = dataf.sort_values('random_col')
    dataf["@@index"] = dataf.index
    dataf["@@partition"] = dataf["@@index"].rank(method='dense', ascending=False).astype(int) % no_partitions
    dataf = dataf.reset_index()
    del dataf['random_col']
    del dataf["@@index"]
    dataf = dataf.reset_index()
    return dataf


def save_partition_to_csv_dataset(dataframe, folder_path, no_partitions):
    """
    Saves the partition to a CSV dataset

    Parameters
    -------------
    dataframe
        Dataframe
    folder_path
        Destination path
    no_partitions
        Number of partitions
    """
    try:
        os.mkdir(folder_path)
    except:
        pass

    for i in range(no_partitions):
        path = os.path.join(folder_path, str(i)+".csv")
        df = dataframe[dataframe["@@partition"] == i]
        df.to_csv(path, index=False)
