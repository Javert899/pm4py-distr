from threading import Thread
from pm4pydistr import configuration
from flask import Flask, request, jsonify
from flask_cors import CORS
from pm4pydistr.slave.variable_container import SlaveVariableContainer
from pm4pydistr.configuration import PARAMETER_USE_TRANSITION, DEFAULT_USE_TRANSITION
from pm4pydistr.configuration import PARAMETER_NO_SAMPLES, DEFAULT_MAX_NO_SAMPLES
import pm4py
import pm4pydistr
from pm4py.util import constants as pm4py_constants
from pm4py.objects.log.util import xes
from pm4pydistr.configuration import PARAMETER_NUM_RET_ITEMS, DEFAULT_WINDOW_SIZE, PARAMETER_WINDOW_SIZE, PARAMETER_START
from pm4pydistr.log_handlers import parquet as parquet_handler
import traceback

import os
import json
import sys

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


@SlaveSocketListener.app.route("/checkVersions", methods=["GET"])
def check_versions():
    return jsonify({"pm4py": pm4py.__version__, "pm4pydistr": pm4pydistr.__version__})


@SlaveSocketListener.app.route("/synchronizeFiles", methods=["POST"])
def synchronize_files():
    keyphrase = request.args.get('keyphrase', type=str)
    if keyphrase == configuration.KEYPHRASE:
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

    if keyphrase == configuration.KEYPHRASE:
        if not session in SlaveVariableContainer.slave.filters:
            SlaveVariableContainer.slave.filters[session] = {}
        try:
            SlaveVariableContainer.slave.filters[session][process] = eval(json.loads(request.data)["filters"])
        except:
            SlaveVariableContainer.slave.filters[session][process] = eval(
                json.loads(request.data.decode('utf-8'))["filters"])
    return jsonify({})


def get_filters_per_session(process, session):
    if session in SlaveVariableContainer.slave.filters:
        if process in SlaveVariableContainer.slave.filters[session]:
            return SlaveVariableContainer.slave.filters[session][process]
    return []


@SlaveSocketListener.app.route("/calculateDfg", methods=["GET"])
def calculate_dfg():
    try:
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

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples
            parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = attribute_key

            returned_dict = parquet_handler.calculate_dfg(SlaveVariableContainer.conf, process,
                                                          SlaveVariableContainer.managed_logs[process],
                                                          parameters=parameters)

            return jsonify({"dfg": returned_dict})
        return jsonify({"dfg": {}})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/calculatePerformanceDfg", methods=["GET"])
def calculate_performance_dfg():
    try:
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

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples
            parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = attribute_key

            returned_dict = parquet_handler.calculate_performance_dfg(SlaveVariableContainer.conf, process,
                                                                      SlaveVariableContainer.managed_logs[process],
                                                                      parameters=parameters)

            return jsonify({"dfg": returned_dict})
        return jsonify({"dfg": {}})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/calculateCompositeObj", methods=["GET"])
def calculate_composite_obj():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)
        attribute_key = request.args.get('attribute_key', type=str, default=xes.DEFAULT_NAME_KEY)
        performance_required = request.args.get('performance_required', type=str, default="False")
        if performance_required == "True":
            performance_required = True
        else:
            performance_required = False

        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples
            parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = attribute_key
            parameters["performance_required"] = performance_required

            returned_dict = parquet_handler.calculate_process_schema_composite_object(SlaveVariableContainer.conf,
                                                                                      process,
                                                                                      SlaveVariableContainer.managed_logs[
                                                                                          process],
                                                                                      parameters=parameters)

            return jsonify({"obj": returned_dict})
        return jsonify({"obj": {}})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getEndActivities", methods=["GET"])
def calculate_end_activities():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)
        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples

            returned_dict = parquet_handler.get_end_activities(SlaveVariableContainer.conf, process,
                                                               SlaveVariableContainer.managed_logs[process],
                                                               parameters=parameters)

            return jsonify({"end_activities": returned_dict})
        return jsonify({"end_activities": {}})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getStartActivities", methods=["GET"])
def calculate_start_activities():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)
        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples

            returned_dict = parquet_handler.get_start_activities(SlaveVariableContainer.conf, process,
                                                                 SlaveVariableContainer.managed_logs[process],
                                                                 parameters=parameters)

            return jsonify({"start_activities": returned_dict})
        return jsonify({"start_activities": {}})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getAttributeValues", methods=["GET"])
def calculate_attribute_values():
    try:
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

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples
            parameters[pm4py_constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = attribute_key

            returned_dict = parquet_handler.get_attribute_values(SlaveVariableContainer.conf, process,
                                                                 SlaveVariableContainer.managed_logs[process],
                                                                 parameters=parameters)

            return jsonify({"values": returned_dict})
        return jsonify({"values": {}})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getAttributesNames", methods=["GET"])
def calculate_attribute_names():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)

        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples

            returned_list = parquet_handler.get_attribute_names(SlaveVariableContainer.conf, process,
                                                                SlaveVariableContainer.managed_logs[process],
                                                                parameters=parameters)

            return jsonify({"names": returned_list})
        return jsonify({"names": {}})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getLogSummary", methods=["GET"])
def calculate_log_summary():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)

        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples

            summary = parquet_handler.get_log_summary(SlaveVariableContainer.conf, process,
                                                      SlaveVariableContainer.managed_logs[process],
                                                      parameters=parameters)

            return jsonify({"summary": summary})
        return jsonify({"summary": {}})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getVariants", methods=["GET"])
def get_variants():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)

        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
        window_size = request.args.get(PARAMETER_WINDOW_SIZE, type=int, default=DEFAULT_WINDOW_SIZE)
        start = request.args.get(PARAMETER_START, type=int, default=0)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples
            parameters[PARAMETER_WINDOW_SIZE] = window_size
            parameters[PARAMETER_START] = start

            returned_dict = parquet_handler.get_variants(SlaveVariableContainer.conf, process,
                                                         SlaveVariableContainer.managed_logs[process],
                                                         parameters=parameters)

            return jsonify(returned_dict)
        return jsonify({"variants": [], "events": 0, "cases": 0})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getCases", methods=["GET"])
def get_cases():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)

        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
        window_size = request.args.get(PARAMETER_WINDOW_SIZE, type=int, default=DEFAULT_WINDOW_SIZE)
        start = request.args.get(PARAMETER_START, type=int, default=0)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples
            parameters[PARAMETER_WINDOW_SIZE] = window_size
            parameters[PARAMETER_START] = start

            returned_dict = parquet_handler.get_cases(SlaveVariableContainer.conf, process,
                                                      SlaveVariableContainer.managed_logs[process],
                                                      parameters=parameters)

            return jsonify(returned_dict)
        return jsonify({"cases_list": [], "events": 0, "cases": 0})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/doCaching", methods=["GET"])
def do_caching():
    try:
        keyphrase = request.args.get('keyphrase', type=str)
        process = request.args.get('process', type=str)

        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

        if keyphrase == configuration.KEYPHRASE:
            parameters = {}
            parameters[PARAMETER_NO_SAMPLES] = no_samples

            parquet_handler.do_caching(SlaveVariableContainer.conf, process,
                                       SlaveVariableContainer.managed_logs[process], parameters=parameters)

        return jsonify({})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getEvents", methods=["GET"])
def get_events():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)
        case_id = request.args.get('case_id', type=str)
        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples
            parameters["case_id"] = case_id

            events = parquet_handler.get_events(SlaveVariableContainer.conf, process,
                                                SlaveVariableContainer.managed_logs[process], parameters=parameters)

            return jsonify({"events": events})
        return jsonify({"events": {}})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getEventsPerDotted", methods=["GET"])
def get_events_per_dotted():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)

        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
        attribute1 = request.args.get("attribute1", type=str)
        attribute2 = request.args.get("attribute2", type=str)
        attribute3 = request.args.get("attribute3", type=str, default=None)
        max_no_ret_items = request.args.get(PARAMETER_NUM_RET_ITEMS, type=int, default=10000)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples
            parameters["attribute1"] = attribute1
            parameters["attribute2"] = attribute2
            parameters["attribute3"] = attribute3
            parameters["max_no_events"] = max_no_ret_items

            returned_dict = parquet_handler.get_events_per_dotted(SlaveVariableContainer.conf, process,
                                                                  SlaveVariableContainer.managed_logs[process],
                                                                  parameters=parameters)

            return jsonify(returned_dict)
        return jsonify({})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getEventsPerCase", methods=["GET"])
def get_events_per_case():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)

        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
        max_no_ret_items = request.args.get(PARAMETER_NUM_RET_ITEMS, type=int, default=100000)

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_NO_SAMPLES] = no_samples
            parameters[PARAMETER_NUM_RET_ITEMS] = max_no_ret_items
            parameters["max_no_of_points_to_sample"] = max_no_ret_items

            returned_dict = parquet_handler.get_events_per_case(SlaveVariableContainer.conf, process,
                                                                SlaveVariableContainer.managed_logs[process],
                                                                parameters=parameters)

            return jsonify({"events_case": returned_dict})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getEventsPerTimeFirst", methods=["GET"])
def get_events_per_time_first():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)

        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
        max_no_ret_items = request.args.get(PARAMETER_NUM_RET_ITEMS, type=int, default=100000)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples
            parameters[PARAMETER_NUM_RET_ITEMS] = max_no_ret_items
            parameters["max_no_of_points_to_sample"] = max_no_ret_items

            returned_list = parquet_handler.get_events_per_time_first(SlaveVariableContainer.conf, process,
                                                                SlaveVariableContainer.managed_logs[process],
                                                                parameters=parameters)

            return jsonify({"points": returned_list})

        return jsonify({})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getEventsPerTime", methods=["GET"])
def get_events_per_time():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)

        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
        max_no_ret_items = request.args.get(PARAMETER_NUM_RET_ITEMS, type=int, default=100000)
        timestamp_key = request.args.get('timestamp_key', type=str, default=xes.DEFAULT_TIMESTAMP_KEY)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples
            parameters[PARAMETER_NUM_RET_ITEMS] = max_no_ret_items
            parameters["max_no_of_points_to_sample"] = max_no_ret_items
            parameters["timestamp_key"] = timestamp_key

            returned_list = parquet_handler.get_events_per_time(SlaveVariableContainer.conf, process,
                                                                SlaveVariableContainer.managed_logs[process],
                                                                parameters=parameters)

            return jsonify({"points": returned_list})

        return jsonify({})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getCaseDuration", methods=["GET"])
def get_case_duration():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)

        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
        max_no_ret_items = request.args.get(PARAMETER_NUM_RET_ITEMS, type=int, default=100000)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples
            parameters[PARAMETER_NUM_RET_ITEMS] = max_no_ret_items
            parameters["max_no_of_points_to_sample"] = max_no_ret_items

            returned_list = parquet_handler.get_case_duration(SlaveVariableContainer.conf, process,
                                                              SlaveVariableContainer.managed_logs[process],
                                                              parameters=parameters)

            return jsonify({"points": returned_list})

        return jsonify({})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/getNumericAttributeValues", methods=["GET"])
def get_numeric_attribute_values():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)

        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
        max_no_ret_items = request.args.get(PARAMETER_NUM_RET_ITEMS, type=int, default=100000)
        attribute_key = request.args.get("attribute_key", type=str)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples
            parameters[PARAMETER_NUM_RET_ITEMS] = max_no_ret_items
            parameters["max_no_of_points_to_sample"] = max_no_ret_items
            parameters["attribute_key"] = attribute_key

            returned_list = parquet_handler.get_numeric_attribute_values(SlaveVariableContainer.conf, process,
                                                                         SlaveVariableContainer.managed_logs[process],
                                                                         parameters=parameters)

            return jsonify({"points": returned_list})

        return jsonify({})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/performAlignments", methods=["POST"])
def perform_alignments():
    try:
        from pm4pydistr.slave import slave

        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)
        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

        try:
            content = json.loads(request.data)
        except:
            content = json.loads(request.data.decode('utf-8'))

        petri_string = content["petri_string"]
        var_list = content["var_list"]
        max_align_time = content["max_align_time"]
        max_align_time_trace = content["max_align_time_trace"]
        align_variant = content["align_variant"]
        classic_alignments_variant = content["classic_alignments_variant"]
        tree_align_variant = content["tree_align_variant"]
        petri_conversion_version = content["petri_conversion_version"]
        require_ilp_computation = content["require_ilp_computation"]

        if keyphrase == configuration.KEYPHRASE:
            parameters = {}
            parameters["max_align_time"] = max_align_time
            parameters["max_align_time_trace"] = max_align_time_trace
            parameters["align_variant"] = align_variant
            parameters["classic_alignments_variant"] = classic_alignments_variant
            parameters["tree_align_variant"] = tree_align_variant
            parameters["petri_conversion_version"] = petri_conversion_version
            parameters["require_ilp_computation"] = require_ilp_computation

            align = slave.perform_alignments(petri_string, var_list, parameters=parameters)

            return jsonify({"alignments": align})

        return jsonify({})
    except:
        exc = traceback.format_exc()
        return exc


@SlaveSocketListener.app.route("/performTbr", methods=["POST"])
def perform_tbr():
    try:
        from pm4pydistr.slave import slave

        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)
        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

        try:
            content = json.loads(request.data)
        except:
            content = json.loads(request.data.decode('utf-8'))

        petri_string = content["petri_string"]
        var_list = content["var_list"]
        enable_parameters_precision = content["enable_parameters_precision"]
        consider_remaining_in_fitness = content["consider_remaining_in_fitness"]

        if keyphrase == configuration.KEYPHRASE:
            parameters = {"enable_parameters_precision": enable_parameters_precision,
                          "consider_remaining_in_fitness": consider_remaining_in_fitness}

            return jsonify({"tbr": slave.perform_token_replay(petri_string, var_list, parameters=parameters)})

        return jsonify({})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/doShutdown", methods=["GET"])
def do_shutdown():
    try:
        keyphrase = request.args.get('keyphrase', type=str)
        process = request.args.get('process', type=str)

        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

        if keyphrase == configuration.KEYPHRASE:
            # do shutdown
            os._exit(0)

        return jsonify({})
    except:
        return traceback.format_exc()


@SlaveSocketListener.app.route("/correlationMiner", methods=["POST"])
def correlation_miner():
    try:
        process = request.args.get('process', type=str)
        keyphrase = request.args.get('keyphrase', type=str)
        session = request.args.get('session', type=str)
        use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
        no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

        if use_transition == "True":
            use_transition = True
        else:
            use_transition = False

        try:
            content = json.loads(request.data)
        except:
            content = json.loads(request.data.decode('utf-8'))

        activities = content["activities"]
        start_timestamp = content["start_timestamp"]
        complete_timestamp = content["complete_timestamp"]

        if keyphrase == configuration.KEYPHRASE:
            filters = get_filters_per_session(process, session)
            parameters = {}
            parameters["filters"] = filters
            parameters["activities"] = activities
            parameters["start_timestamp"] = start_timestamp
            parameters["complete_timestamp"] = complete_timestamp

            parameters[PARAMETER_USE_TRANSITION] = use_transition
            parameters[PARAMETER_NO_SAMPLES] = no_samples

            ret = parquet_handler.correlation_miner(SlaveVariableContainer.conf, process,
                                                                         SlaveVariableContainer.managed_logs[process],
                                                                         parameters=parameters)

            return jsonify(ret)

        return jsonify({"PS_matrix": [], "duration_matrix": []})
    except:
        exc = traceback.format_exc()
        return exc
