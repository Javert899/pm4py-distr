from pm4py.objects.log.importer.parquet import factory as parquet_importer

def apply(path, parameters=None):
    if parameters is None:
        parameters = {}

    list_parquets = parquet_importer.apply(path)