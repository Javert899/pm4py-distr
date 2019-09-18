from threading import Thread
from pm4pydistr.configuration import KEYPHRASE
from flask import Flask, request, jsonify
from flask_cors import CORS
from pm4pydistr.slave.variable_container import SlaveVariableContainer
from pm4pydistr.configuration import PARAMETER_USE_TRANSITION, DEFAULT_USE_TRANSITION
from pm4pydistr.configuration import PARAMETER_NO_SAMPLES, DEFAULT_MAX_NO_SAMPLES
from pm4py.util import constants as pm4py_constants
from pm4py.objects.log.util import xes

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
        try:
            json_content = json.loads(request.data)
        except:
            json_content = json.loads(request.data.decode('utf-8'))
        for log_folder in json_content["logs"]:
            SlaveVariableContainer.managed_logs[log_folder] = None
            SlaveVariableContainer.managed_logs[log_folder] = []

            if log_folder not in os.listdir(SlaveVariableContainer.conf):
                SlaveVariableContainer.slave.create_folder(log_folder)
            for log_name in json_content["logs"][log_folder]:
                SlaveVariableContainer.slave.load_log(log_folder, log_name)
                SlaveVariableContainer.managed_logs[log_folder].append(log_name)
    return jsonify({})


@SlaveSocketListener.app.route("/setFilters", methods=["POST"])
def set_filters():
    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    if keyphrase == KEYPHRASE:
        if not session in SlaveVariableContainer.slave.filters:
            SlaveVariableContainer.slave.filters[session] = {}
        SlaveVariableContainer.slave.filters[session][process] = eval(json.loads(request.data)["filters"])
    return jsonify({})

def get_filters_per_session(process, session):
    if session in SlaveVariableContainer.slave.filters:
        if process in SlaveVariableContainer.slave.filters[session]:
            return SlaveVariableContainer.slave.filters[session][process]
    return []

@SlaveSocketListener.app.route("/calculateDfg", methods=["GET"])
def calculate_dfg():
    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)
    attribute_key = request.args.get('attribute_key', type=str, default=xes.DEFAULT_NAME_KEY)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if use_transition == "True":
        use_transition = True
    else:
        use_transition = False

    if keyphrase == KEYPHRASE:
        filters = get_filters_per_session(process, session)
        parameters = {}
        parameters["filters"] = filters
        parameters[PARAMETER_USE_TRANSITION] = use_transition
        parameters[PARAMETER_NO_SAMPLES] = no_samples
        parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = attribute_key

        returned_dict = parquet_handler.calculate_dfg(SlaveVariableContainer.conf, process, SlaveVariableContainer.managed_logs[process], parameters=parameters)

        return jsonify({"dfg": returned_dict})
    return jsonify({"dfg": {}})


@SlaveSocketListener.app.route("/getEndActivities", methods=["GET"])
def calculate_end_activities():
    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)
    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if use_transition == "True":
        use_transition = True
    else:
        use_transition = False

    if keyphrase == KEYPHRASE:
        filters = get_filters_per_session(process, session)
        parameters = {}
        parameters["filters"] = filters
        parameters[PARAMETER_USE_TRANSITION] = use_transition
        parameters[PARAMETER_NO_SAMPLES] = no_samples

        returned_dict = parquet_handler.get_end_activities(SlaveVariableContainer.conf, process, SlaveVariableContainer.managed_logs[process], parameters=parameters)

        return jsonify({"end_activities": returned_dict})
    return jsonify({"end_activities": {}})


@SlaveSocketListener.app.route("/getStartActivities", methods=["GET"])
def calculate_start_activities():
    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)
    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if use_transition == "True":
        use_transition = True
    else:
        use_transition = False

    if keyphrase == KEYPHRASE:
        filters = get_filters_per_session(process, session)
        parameters = {}
        parameters["filters"] = filters
        parameters[PARAMETER_USE_TRANSITION] = use_transition
        parameters[PARAMETER_NO_SAMPLES] = no_samples

        returned_dict = parquet_handler.get_start_activities(SlaveVariableContainer.conf, process, SlaveVariableContainer.managed_logs[process], parameters=parameters)

        return jsonify({"start_activities": returned_dict})
    return jsonify({"start_activities": {}})


@SlaveSocketListener.app.route("/getAttributeValues", methods=["GET"])
def calculate_attribute_values():
    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)
    attribute_key = request.args.get('attribute_key', type=str, default=xes.DEFAULT_NAME_KEY)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if use_transition == "True":
        use_transition = True
    else:
        use_transition = False

    if keyphrase == KEYPHRASE:
        filters = get_filters_per_session(process, session)
        parameters = {}
        parameters["filters"] = filters
        parameters[PARAMETER_USE_TRANSITION] = use_transition
        parameters[PARAMETER_NO_SAMPLES] = no_samples
        parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = attribute_key

        returned_dict = parquet_handler.get_attribute_values(SlaveVariableContainer.conf, process, SlaveVariableContainer.managed_logs[process], parameters=parameters)

        return jsonify({"values": returned_dict})
    return jsonify({"values": {}})


@SlaveSocketListener.app.route("/getAttributesNames", methods=["GET"])
def calculate_attribute_names():
    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if use_transition == "True":
        use_transition = True
    else:
        use_transition = False

    if keyphrase == KEYPHRASE:
        filters = get_filters_per_session(process, session)
        parameters = {}
        parameters["filters"] = filters
        parameters[PARAMETER_USE_TRANSITION] = use_transition
        parameters[PARAMETER_NO_SAMPLES] = no_samples

        returned_list = parquet_handler.get_attribute_names(SlaveVariableContainer.conf, process, SlaveVariableContainer.managed_logs[process], parameters=parameters)

        return jsonify({"names": returned_list})
    return jsonify({"names": {}})


@SlaveSocketListener.app.route("/getLogSummary", methods=["GET"])
def calculate_log_summary():
    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if use_transition == "True":
        use_transition = True
    else:
        use_transition = False

    if keyphrase == KEYPHRASE:
        filters = get_filters_per_session(process, session)
        parameters = {}
        parameters["filters"] = filters
        parameters[PARAMETER_USE_TRANSITION] = use_transition
        parameters[PARAMETER_NO_SAMPLES] = no_samples

        summary = parquet_handler.get_log_summary(SlaveVariableContainer.conf, process, SlaveVariableContainer.managed_logs[process], parameters=parameters)

        return jsonify({"summary": summary})
    return jsonify({"summary": {}})
