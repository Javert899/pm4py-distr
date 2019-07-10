from threading import Thread
from pm4pydistr.configuration import KEYPHRASE
from flask import Flask, request, jsonify
from flask_cors import CORS
from pm4pydistr.slave.variable_container import SlaveVariableContainer


from pm4pydistr.log_handlers import parquet as parquet_handler

import os
import json

import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

class SlaveSocketListener(Thread):
    app = Flask(__name__)
    CORS(app)

    def __init__(self, slave, host, port, master_host, master_port, conf):
        SlaveVariableContainer.slave = slave
        SlaveVariableContainer.host = host
        SlaveVariableContainer.port = port
        SlaveVariableContainer.master_host = master_host
        SlaveVariableContainer.master_port = master_port
        SlaveVariableContainer.conf = conf

        Thread.__init__(self)

    def run(self):
        self.app.run(host="0.0.0.0", port=SlaveVariableContainer.port, threaded=True)


@SlaveSocketListener.app.route("/synchronizeFiles", methods=["POST"])
def synchronize_files():
    keyphrase = request.args.get('keyphrase', type=str)
    if keyphrase == KEYPHRASE:
        json_content = json.loads(request.data)
        for log_folder in json_content["logs"]:
            SlaveVariableContainer.managed_logs[log_folder] = None
            SlaveVariableContainer.managed_logs[log_folder] = []

            if log_folder not in os.listdir(SlaveVariableContainer.conf):
                SlaveVariableContainer.slave.create_folder(log_folder)
            for log_name in json_content["logs"][log_folder]:
                SlaveVariableContainer.slave.load_log(log_folder, log_name)
                SlaveVariableContainer.managed_logs[log_folder].append(log_name)
    return jsonify({})


@SlaveSocketListener.app.route("/calculateDfg", methods=["GET"])
def calculate_dfg():
    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    if keyphrase == KEYPHRASE:
        returned_dict = parquet_handler.calculate_dfg(SlaveVariableContainer.conf, process, SlaveVariableContainer.managed_logs[process])

        return jsonify({"dfg": returned_dict})
    return jsonify({"dfg": {}})


@SlaveSocketListener.app.route("/getEndActivities", methods=["GET"])
def calculate_end_activities():
    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    if keyphrase == KEYPHRASE:
        returned_dict = parquet_handler.get_end_activities(SlaveVariableContainer.conf, process, SlaveVariableContainer.managed_logs[process])

        return jsonify({"end_activities": returned_dict})
    return jsonify({"end_activities": {}})


@SlaveSocketListener.app.route("/getStartActivities", methods=["GET"])
def calculate_start_activities():
    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    if keyphrase == KEYPHRASE:
        returned_dict = parquet_handler.get_start_activities(SlaveVariableContainer.conf, process, SlaveVariableContainer.managed_logs[process])

        return jsonify({"start_activities": returned_dict})
    return jsonify({"start_activities": {}})
