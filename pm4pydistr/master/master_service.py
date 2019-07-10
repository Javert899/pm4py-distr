from threading import Thread
from pm4pydistr.configuration import KEYPHRASE
from flask import Flask, request, jsonify
from flask_cors import CORS
from random import randrange
from time import time
from pm4pydistr.master.variable_container import MasterVariableContainer
from pm4pydistr.master.db_manager import DbManager

import logging

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


@MasterSocketListener.app.route("/registerSlave", methods=["GET"])
def register_slave():
    keyphrase = request.args.get('keyphrase', type=str)
    ip = request.args.get('ip', type=str)
    port = request.args.get('port', type=str)
    conf = request.args.get('conf', type=str)

    if keyphrase == KEYPHRASE:
        id = [randrange(0, 10), randrange(0, 10), randrange(0, 10), randrange(0, 10), randrange(0, 10),
              randrange(0, 10), randrange(0, 10)]
        id = MasterVariableContainer.dbmanager.insert_slave_into_db(conf, id)
        MasterVariableContainer.master.slaves[str(id)] = [conf, ip, port, time()]
        return jsonify({"id": str(id)})


@MasterSocketListener.app.route("/updateSlave", methods=["GET"])
def update_slave():
    keyphrase = request.args.get('keyphrase', type=str)
    id = request.args.get('id', type=str)
    ip = request.args.get('ip', type=str)
    port = request.args.get('port', type=str)
    conf = request.args.get('conf', type=str)

    if keyphrase == KEYPHRASE:
        MasterVariableContainer.master.slaves[id] = [conf, ip, port, time()]
        return jsonify({"id": id})


@MasterSocketListener.app.route("/pingFromSlave", methods=["GET"])
def ping_from_slave():
    keyphrase = request.args.get('keyphrase', type=str)
    id = request.args.get('id', type=str)
    conf = request.args.get('conf', type=str)

    if keyphrase == KEYPHRASE:
        MasterVariableContainer.master.slaves[id][3] = time()
        return jsonify({"id": id})


@MasterSocketListener.app.route("/doLogAssignment", methods=["GET"])
def do_log_assingment():
    keyphrase = request.args.get('keyphrase', type=str)

    if keyphrase == KEYPHRASE:
        MasterVariableContainer.master.do_assignment()
        MasterVariableContainer.master.make_slaves_load()

    return jsonify({})


@MasterSocketListener.app.route("/calculateDfg", methods=["GET"])
def calculate_dfg():
    keyphrase = request.args.get('keyphrase', type=str)
    process = request.args.get('process', type=str)

    if keyphrase == KEYPHRASE:
        overall_dfg = MasterVariableContainer.master.calculate_dfg(process)

        return jsonify({"dfg": overall_dfg})

    return jsonify({})


@MasterSocketListener.app.route("/getEndActivities", methods=["GET"])
def calculate_end_activities():
    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    if keyphrase == KEYPHRASE:
        overall_ea = MasterVariableContainer.master.get_end_activities(process)

        return jsonify({"end_activities": overall_ea})

    return jsonify({})


@MasterSocketListener.app.route("/getStartActivities", methods=["GET"])
def calculate_start_activities():
    process = request.args.get('process', type=str)
    keyphrase = request.args.get('keyphrase', type=str)
    if keyphrase == KEYPHRASE:
        overall_sa = MasterVariableContainer.master.get_start_activities(process)

        return jsonify({"start_activities": overall_sa})
    return jsonify({"start_activities": {}})


@MasterSocketListener.app.route("/getSlavesList", methods=["GET"])
def get_slaves_list():
    keyphrase = request.args.get('keyphrase', type=str)
    if keyphrase == KEYPHRASE:
        return jsonify({"slaves": MasterVariableContainer.master.slaves})
    return jsonify({})


@MasterSocketListener.app.route("/getSublogsId", methods=["GET"])
def get_sublogs_id():
    keyphrase = request.args.get('keyphrase', type=str)
    if keyphrase == KEYPHRASE:
        return jsonify({"sublogs_id": MasterVariableContainer.master.sublogs_id})
    return jsonify({})


@MasterSocketListener.app.route("/getSublogsCorrespondence", methods=["GET"])
def get_sublogs_correspondence():
    keyphrase = request.args.get('keyphrase', type=str)
    if keyphrase == KEYPHRASE:
        return jsonify({"sublogs_correspondence": MasterVariableContainer.master.sublogs_correspondence})
    return jsonify({})

