from threading import Thread
from pm4pydistr.configuration import KEYPHRASE
from flask import Flask, request, jsonify
from flask_cors import CORS
from random import randrange
from time import time
from pm4pydistr.master.variable_container import MasterVariableContainer


class MasterSocketListener(Thread):
    app = Flask(__name__)
    CORS(app)

    def __init__(self, port, master):
        MasterVariableContainer.port = port
        MasterVariableContainer.master = master
        Thread.__init__(self)

    def run(self):
        self.app.run(host="0.0.0.0", port=MasterVariableContainer.port, threaded=True)


@MasterSocketListener.app.route("/registerSlave", methods=["GET"])
def register_slave():
    keyphrase = request.args.get('keyphrase', type=str)
    ip = request.args.get('ip', type=str)
    port = request.args.get('port', type=str)

    if keyphrase == KEYPHRASE:
        id = [randrange(0, 10), randrange(0, 10), randrange(0, 10), randrange(0, 10), randrange(0, 10),
              randrange(0, 10), randrange(0, 10)]
        MasterVariableContainer.master.slaves[str(id)] = [ip, port, time()]
        return jsonify({"id": str(id)})


@MasterSocketListener.app.route("/updateSlave", methods=["GET"])
def update_slave():
    keyphrase = request.args.get('keyphrase', type=str)
    id = request.args.get('id', type=str)
    ip = request.args.get('ip', type=str)
    port = request.args.get('port', type=str)

    if keyphrase == KEYPHRASE:
        MasterVariableContainer.master.slaves[id] = [ip, port, time()]
        return jsonify({"id": id})


@MasterSocketListener.app.route("/pingFromSlave", methods=["GET"])
def ping_from_slave():
    keyphrase = request.args.get('keyphrase', type=str)
    id = request.args.get('id', type=str)

    if keyphrase == KEYPHRASE:
        MasterVariableContainer.master.slaves[id][2] = time()
        return jsonify({"id": id})
