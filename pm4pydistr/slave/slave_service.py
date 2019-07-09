from threading import Thread
from pm4pydistr.configuration import KEYPHRASE
from flask import Flask, request, jsonify
from flask_cors import CORS
from random import randrange
from time import time
from pm4pydistr.slave.variable_container import SlaveVariableContainer

from pm4py.objects.log.importer.parquet import factory as parquet_importer
from pm4py.algo.filtering.common.filtering_constants import CASE_CONCEPT_NAME
from pm4py.objects.log.util.xes import DEFAULT_NAME_KEY
from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics

from collections import Counter

import os

class SlaveSocketListener(Thread):
    app = Flask(__name__)
    CORS(app)

    def __init__(self, host, port, master_host, master_port, conf):
        SlaveVariableContainer.host = host
        SlaveVariableContainer.port = port
        SlaveVariableContainer.master_host = master_host
        SlaveVariableContainer.master_port = master_port
        SlaveVariableContainer.conf = conf
        Thread.__init__(self)

    def run(self):
        self.app.run(host="0.0.0.0", port=SlaveVariableContainer.port, threaded=True)


@SlaveSocketListener.app.route("/calculateDfg", methods=["GET"])
def calculate_dfg():
    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    if keyphrase == KEYPHRASE:
        folder = os.path.join(SlaveVariableContainer.conf, process)
        parquet_list = parquet_importer.get_list_parquet(folder)
        overall_dfg = Counter()
        for pq in parquet_list:
            df = parquet_importer.apply(pq, parameters={"columns": [CASE_CONCEPT_NAME, DEFAULT_NAME_KEY]})
            dfg = Counter(df_statistics.get_dfg_graph(df, sort_timestamp_along_case_id=False, sort_caseid_required=False))
            overall_dfg = overall_dfg + dfg
        returned_dict = {}
        for el in overall_dfg:
            returned_dict[el[0]+"@@"+el[1]] = overall_dfg[el]

        return jsonify({"dfg": returned_dict})
    return jsonify({"dfg": {}})
