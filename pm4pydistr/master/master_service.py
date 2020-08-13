from threading import Thread
from pm4pydistr import configuration
from flask import Flask, request, jsonify
from flask_cors import CORS
from random import randrange
from time import time, sleep
from pm4pydistr.configuration import PARAMETER_USE_TRANSITION, DEFAULT_USE_TRANSITION
from pm4pydistr.configuration import PARAMETER_NO_SAMPLES, DEFAULT_MAX_NO_SAMPLES
from pm4pydistr.configuration import PARAMETER_NUM_RET_ITEMS, DEFAULT_WINDOW_SIZE, PARAMETER_WINDOW_SIZE, \
    PARAMETER_START
from pm4pydistr.master.variable_container import MasterVariableContainer
from pm4pydistr.master.db_manager import DbManager
import pm4py
import pm4pydistr
from pm4py.objects.log.util import xes

import logging, json
import sys

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)


class MasterSocketListener(Thread):
    app = Flask(__name__)
    CORS(app)

    def __init__(self, master, port, conf):
        MasterVariableContainer.port = port
        MasterVariableContainer.master = master
        MasterVariableContainer.conf = conf
        MasterVariableContainer.dbmanager = DbManager(MasterVariableContainer.conf)

        Thread.__init__(self)

    def run(self):
        self.app.run(host="0.0.0.0", port=MasterVariableContainer.port, threaded=True)


def check_master_initialized():
    while not MasterVariableContainer.master_initialization_done:
        # sleep a while till the master is fine
        sleep(0.55)


def except_if_not_slave_loading_requested():
    if not MasterVariableContainer.slave_loading_requested:
        raise Exception("slave loading not requested")


def get_slaves_count():
    slaves_count = len(MasterVariableContainer.master.slaves)
    finished_slaves = sum(t.slave_finished for t in MasterVariableContainer.assign_request_threads)

    return slaves_count, finished_slaves


def wait_till_slave_load_requested():
    while True:
        slaves_count, finished_slaves = get_slaves_count()

        if not finished_slaves >= slaves_count:
            sleep(0.55)
        else:
            break


@MasterSocketListener.app.route("/registerSlave", methods=["GET"])
def register_slave():
    check_master_initialized()

    keyphrase = request.args.get('keyphrase', type=str)
    ip = request.args.get('ip', type=str)
    port = request.args.get('port', type=str)
    conf = request.args.get('conf', type=str)

    if keyphrase == configuration.KEYPHRASE:
        id = [randrange(0, 10), randrange(0, 10), randrange(0, 10), randrange(0, 10), randrange(0, 10),
              randrange(0, 10), randrange(0, 10)]
        id = MasterVariableContainer.dbmanager.insert_slave_into_db(conf, id)
        MasterVariableContainer.master.slaves[str(id)] = [conf, ip, port, time()]
        return jsonify({"id": str(id)})


@MasterSocketListener.app.route("/updateSlave", methods=["GET"])
def update_slave():
    check_master_initialized()

    keyphrase = request.args.get('keyphrase', type=str)
    id = request.args.get('id', type=str)
    ip = request.args.get('ip', type=str)
    port = request.args.get('port', type=str)
    conf = request.args.get('conf', type=str)

    if keyphrase == configuration.KEYPHRASE:
        MasterVariableContainer.master.slaves[id] = [conf, ip, port, time()]
        return jsonify({"id": id})


@MasterSocketListener.app.route("/pingFromSlave", methods=["GET"])
def ping_from_slave():
    check_master_initialized()

    keyphrase = request.args.get('keyphrase', type=str)
    id = request.args.get('id', type=str)
    conf = request.args.get('conf', type=str)

    if keyphrase == configuration.KEYPHRASE:
        MasterVariableContainer.master.slaves[id][3] = time()
        return jsonify({"id": id})


@MasterSocketListener.app.route("/getLoadingStatus", methods=["GET"])
def get_loading_status():
    check_master_initialized()

    keyphrase = request.args.get('keyphrase', type=str)

    if keyphrase == configuration.KEYPHRASE:
        slaves_count, finished_slaves = get_slaves_count()
        return jsonify({"keyphrase_correct": True, "first_loading_done": MasterVariableContainer.first_loading_done,
                        "log_assignment_done": MasterVariableContainer.log_assignment_done,
                        "slave_loading_requested": MasterVariableContainer.slave_loading_requested,
                        "slaves_count": slaves_count, "finished_slaves": finished_slaves})

    return jsonify({"keyphrase_correct": False})


@MasterSocketListener.app.route("/doLogAssignment", methods=["GET"])
def do_log_assignment():
    check_master_initialized()

    keyphrase = request.args.get('keyphrase', type=str)

    if keyphrase == configuration.KEYPHRASE:
        MasterVariableContainer.master.do_assignment()
        MasterVariableContainer.master.make_slaves_load()

    return jsonify({})


@MasterSocketListener.app.route("/checkVersions", methods=["GET"])
def check_versions():
    check_master_initialized()

    return jsonify({"pm4py": pm4py.__version__, "pm4pydistr": pm4pydistr.__version__})


@MasterSocketListener.app.route("/getSlavesList", methods=["GET"])
def get_slaves_list():
    check_master_initialized()

    keyphrase = request.args.get('keyphrase', type=str)

    if keyphrase == configuration.KEYPHRASE:
        return jsonify(
            {"slaves": MasterVariableContainer.master.slaves, "uuid": MasterVariableContainer.master.unique_identifier})
    return jsonify({})


@MasterSocketListener.app.route("/getSublogsId", methods=["GET"])
def get_sublogs_id():
    check_master_initialized()

    keyphrase = request.args.get('keyphrase', type=str)
    if keyphrase == configuration.KEYPHRASE:
        return jsonify({"sublogs_id": MasterVariableContainer.master.sublogs_id})
    return jsonify({})


@MasterSocketListener.app.route("/getSublogsCorrespondence", methods=["GET"])
def get_sublogs_correspondence():
    check_master_initialized()

    keyphrase = request.args.get('keyphrase', type=str)
    if keyphrase == configuration.KEYPHRASE:
        return jsonify({"sublogs_correspondence": MasterVariableContainer.master.sublogs_correspondence})
    return jsonify({})


@MasterSocketListener.app.route("/setFilters", methods=["POST"])
def set_filters():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)
    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    try:
        filters = json.loads(request.data)["filters"]
    except:
        filters = json.loads(request.data.decode('utf-8'))["filters"]
    if keyphrase == configuration.KEYPHRASE:
        MasterVariableContainer.master.set_filter(session, process, filters, use_transition, no_samples)
    return jsonify({})


@MasterSocketListener.app.route("/doCaching", methods=["GET"])
def do_caching():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    keyphrase = request.args.get('keyphrase', type=str)
    process = request.args.get('process', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if keyphrase == configuration.KEYPHRASE:
        res = MasterVariableContainer.master.do_caching(session, process, use_transition, no_samples)

    return jsonify({})


@MasterSocketListener.app.route("/calculateDfg", methods=["GET"])
def calculate_dfg():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    keyphrase = request.args.get('keyphrase', type=str)
    process = request.args.get('process', type=str)
    session = request.args.get('session', type=str)
    attribute_key = request.args.get('attribute_key', type=str, default=xes.DEFAULT_NAME_KEY)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if keyphrase == configuration.KEYPHRASE:
        overall_dfg = MasterVariableContainer.master.calculate_dfg(session, process, use_transition, no_samples,
                                                                   attribute_key)

        return jsonify({"dfg": overall_dfg})

    return jsonify({})


@MasterSocketListener.app.route("/calculatePerformanceDfg", methods=["GET"])
def calculate_performance_dfg():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    keyphrase = request.args.get('keyphrase', type=str)
    process = request.args.get('process', type=str)
    session = request.args.get('session', type=str)
    attribute_key = request.args.get('attribute_key', type=str, default=xes.DEFAULT_NAME_KEY)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if keyphrase == configuration.KEYPHRASE:
        overall_dfg = MasterVariableContainer.master.calculate_performance_dfg(session, process, use_transition,
                                                                               no_samples, attribute_key)

        return jsonify({"dfg": overall_dfg})

    return jsonify({})


@MasterSocketListener.app.route("/calculateCompositeObj", methods=["GET"])
def calculate_composite_obj():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    keyphrase = request.args.get('keyphrase', type=str)
    process = request.args.get('process', type=str)
    session = request.args.get('session', type=str)
    attribute_key = request.args.get('attribute_key', type=str, default=xes.DEFAULT_NAME_KEY)
    performance_required = request.args.get('performance_required', type=str, default="False")
    if performance_required == "True":
        performance_required = True
    else:
        performance_required = False

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if keyphrase == configuration.KEYPHRASE:
        overall_obj = MasterVariableContainer.master.calculate_composite_obj(session, process, use_transition,
                                                                             no_samples, attribute_key,
                                                                             performance_required=performance_required)

        return jsonify({"obj": overall_obj})

    return jsonify({})


@MasterSocketListener.app.route("/getEndActivities", methods=["GET"])
def calculate_end_activities():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)
    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if keyphrase == configuration.KEYPHRASE:
        overall_ea = MasterVariableContainer.master.get_end_activities(session, process, use_transition, no_samples)

        return jsonify({"end_activities": overall_ea})

    return jsonify({})


@MasterSocketListener.app.route("/getStartActivities", methods=["GET"])
def calculate_start_activities():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)
    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if keyphrase == configuration.KEYPHRASE:
        overall_sa = MasterVariableContainer.master.get_start_activities(session, process, use_transition, no_samples)

        return jsonify({"start_activities": overall_sa})
    return jsonify({"start_activities": {}})


@MasterSocketListener.app.route("/getAttributeValues", methods=["GET"])
def calculate_attribute_values():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)
    attribute_key = request.args.get('attribute_key', type=str, default=xes.DEFAULT_NAME_KEY)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if keyphrase == configuration.KEYPHRASE:
        values = MasterVariableContainer.master.get_attribute_values(session, process, use_transition, no_samples,
                                                                     attribute_key)

        return jsonify({"values": values})

    return jsonify({})


@MasterSocketListener.app.route("/getAttributesNames", methods=["GET"])
def calculate_attributes_names():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if keyphrase == configuration.KEYPHRASE:
        names = MasterVariableContainer.master.get_attributes_names(session, process, use_transition, no_samples)

        return jsonify({"names": names})
    return jsonify({"names": {}})


@MasterSocketListener.app.route("/getLogSummary", methods=["GET"])
def calculate_log_summary():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if keyphrase == configuration.KEYPHRASE:
        summary = MasterVariableContainer.master.get_log_summary(session, process, use_transition, no_samples)

        return jsonify({"summary": summary})
    return jsonify({"summary": {}})


@MasterSocketListener.app.route("/getVariants", methods=["GET"])
def get_variants():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
    window_size = request.args.get(PARAMETER_WINDOW_SIZE, type=int, default=DEFAULT_WINDOW_SIZE)
    start = request.args.get(PARAMETER_START, type=int, default=0)

    if keyphrase == configuration.KEYPHRASE:
        variants = MasterVariableContainer.master.get_variants(session, process, use_transition, no_samples,
                                                               start=start, window_size=window_size)

        return jsonify(variants)
    return jsonify({"variants": [], "events": 0, "cases": 0})


@MasterSocketListener.app.route("/getCases", methods=["GET"])
def get_cases():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
    window_size = request.args.get(PARAMETER_WINDOW_SIZE, type=int, default=DEFAULT_WINDOW_SIZE)
    start = request.args.get(PARAMETER_START, type=int, default=0)

    if keyphrase == configuration.KEYPHRASE:
        cases = MasterVariableContainer.master.get_cases(session, process, use_transition, no_samples,
                                                         window_size=window_size, start=start)

        return jsonify(cases)
    return jsonify({"cases_list": [], "events": 0, "cases": 0})


@MasterSocketListener.app.route("/getEvents", methods=["GET"])
def get_events():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)
    case_id = request.args.get('case_id', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if keyphrase == configuration.KEYPHRASE:
        events = MasterVariableContainer.master.get_events(session, process, use_transition, no_samples, case_id)

        return jsonify({"events": events})

    return jsonify({})


@MasterSocketListener.app.route("/getEventsPerDotted", methods=["GET"])
def get_events_per_dotted():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
    max_no_ret_items = request.args.get(PARAMETER_NUM_RET_ITEMS, type=int, default=DEFAULT_WINDOW_SIZE)

    attribute1 = request.args.get("attribute1", type=str)
    attribute2 = request.args.get("attribute2", type=str)
    attribute3 = request.args.get("attribute3", type=str, default=None)

    if keyphrase == configuration.KEYPHRASE:
        ret = MasterVariableContainer.master.get_events_per_dotted(session, process, use_transition, no_samples,
                                                                   attribute1, attribute2, attribute3,
                                                                   max_ret_items=max_no_ret_items)

        return jsonify({"traces": ret[0], "types": ret[1], "attributes": ret[2], "third_unique_values": ret[3]})

    return jsonify({})


@MasterSocketListener.app.route("/getEventsPerCase", methods=["GET"])
def get_events_per_case():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
    max_no_ret_items = request.args.get(PARAMETER_NUM_RET_ITEMS, type=int, default=DEFAULT_WINDOW_SIZE)

    if keyphrase == configuration.KEYPHRASE:
        events = MasterVariableContainer.master.get_events_per_case(session, process, use_transition, no_samples,
                                                                    max_ret_items=max_no_ret_items)

        return jsonify({"events_case": events})

    return jsonify({})


@MasterSocketListener.app.route("/getEventsPerTime", methods=["GET"])
def get_events_per_time():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
    max_no_ret_items = request.args.get(PARAMETER_NUM_RET_ITEMS, type=int, default=DEFAULT_WINDOW_SIZE)
    timestamp_key = request.args.get('timestamp_key', type=str, default=xes.DEFAULT_TIMESTAMP_KEY)

    if keyphrase == configuration.KEYPHRASE:
        points = MasterVariableContainer.master.get_events_per_time(session, process, use_transition, no_samples,
                                                                    max_ret_items=max_no_ret_items,
                                                                    timestamp_key=timestamp_key)

        return jsonify({"points": points})

    return jsonify({})


@MasterSocketListener.app.route("/getEventsPerTimeFirst", methods=["GET"])
def get_events_per_time_first():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
    max_no_ret_items = request.args.get(PARAMETER_NUM_RET_ITEMS, type=int, default=DEFAULT_WINDOW_SIZE)

    if keyphrase == configuration.KEYPHRASE:
        points = MasterVariableContainer.master.get_events_per_time_first(session, process, use_transition, no_samples,
                                                                          max_ret_items=max_no_ret_items)

        return jsonify({"points": points})

    return jsonify({})


@MasterSocketListener.app.route("/getCaseDuration", methods=["GET"])
def get_case_duration():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
    max_no_ret_items = request.args.get(PARAMETER_NUM_RET_ITEMS, type=int, default=DEFAULT_WINDOW_SIZE)

    if keyphrase == configuration.KEYPHRASE:
        points = MasterVariableContainer.master.get_case_duration(session, process, use_transition, no_samples,
                                                                  max_ret_items=max_no_ret_items)

        return jsonify({"points": points})

    return jsonify({})


@MasterSocketListener.app.route("/getNumericAttributeValues", methods=["GET"])
def get_numeric_attribute_values():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)
    max_no_ret_items = request.args.get(PARAMETER_NUM_RET_ITEMS, type=int, default=DEFAULT_WINDOW_SIZE)

    attribute_key = request.args.get("attribute_key", type=str)

    if keyphrase == configuration.KEYPHRASE:
        points = MasterVariableContainer.master.get_numeric_attribute_values(session, process, use_transition,
                                                                             no_samples, attribute_key,
                                                                             max_ret_items=max_no_ret_items)

        return jsonify({"points": points})

    return jsonify({})


@MasterSocketListener.app.route("/performAlignments", methods=["POST"])
def perform_alignments():
    try:
        check_master_initialized()
        except_if_not_slave_loading_requested()
        wait_till_slave_load_requested()

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
        max_thread_join_time = content["max_thread_join_time"]
        align_variant = content["align_variant"]
        classic_alignments_variant = content["classic_alignments_variant"]
        tree_align_variant = content["tree_align_variant"]
        petri_conversion_version = content["petri_conversion_version"]
        require_ilp_computation = content["require_ilp_computation"]

        if keyphrase == configuration.KEYPHRASE:
            alignments = MasterVariableContainer.master.perform_alignments(session, process, use_transition, no_samples,
                                                                           petri_string, var_list,
                                                                           max_align_time=max_align_time,
                                                                           max_align_time_trace=max_align_time_trace,
                                                                           align_variant=align_variant,
                                                                           classic_alignments_variant=classic_alignments_variant,
                                                                           tree_align_variant=tree_align_variant,
                                                                           petri_conversion_version=petri_conversion_version,
                                                                           require_ilp_computation=require_ilp_computation,
                                                                           max_thread_join_time=max_thread_join_time)
            return jsonify({"alignments": alignments})

        return jsonify({})
    except:
        import traceback
        exc = traceback.format_exc()
        return exc


@MasterSocketListener.app.route("/performTbr", methods=["POST"])
def perform_tbr():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

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
    enable_parameters_precision = content[
        "enable_parameters_precision"] if "enable_parameters_precision" in content else False
    consider_remaining_in_fitness = content[
        "consider_remaining_in_fitness"] if "consider_remaining_in_fitness" in content else False

    if keyphrase == configuration.KEYPHRASE:
        tbr = MasterVariableContainer.master.perform_tbr(session, process, use_transition, no_samples, petri_string,
                                                         var_list, enable_parameters_precision,
                                                         consider_remaining_in_fitness)
        return jsonify({"tbr": tbr})

    return jsonify({})


@MasterSocketListener.app.route("/doShutdown", methods=["GET"])
def do_shutdown():
    check_master_initialized()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)
    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    if keyphrase == configuration.KEYPHRASE:
        MasterVariableContainer.master.perform_shutdown(session, process, use_transition, no_samples)

    return jsonify({})


@MasterSocketListener.app.route("/correlationMiner", methods=["POST"])
def correlation_miner():
    check_master_initialized()
    except_if_not_slave_loading_requested()
    wait_till_slave_load_requested()

    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    session = request.args.get('session', type=str)

    use_transition = request.args.get(PARAMETER_USE_TRANSITION, type=str, default=str(DEFAULT_USE_TRANSITION))
    no_samples = request.args.get(PARAMETER_NO_SAMPLES, type=int, default=DEFAULT_MAX_NO_SAMPLES)

    try:
        try:
            content = json.loads(request.data)
        except:
            content = json.loads(request.data.decode('utf-8'))

        activities = content["activities"]
        start_timestamp = content["start_timestamp"]
        end_timestamp = content["complete_timestamp"]

        if keyphrase == configuration.KEYPHRASE:
            ret = MasterVariableContainer.master.correlation_miner(session, process, use_transition, no_samples,
                                                                   activities,
                                                                   start_timestamp, end_timestamp)
            return jsonify(ret)

        return jsonify({"PS_matrix": [], "duration_matrix": []})
    except:
        import traceback
        exc = traceback.format_exc()
        return exc
