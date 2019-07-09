from threading import Thread
from pm4pydistr.configuration import PARAMETERS_PORT, KEYPHRASE
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
        id = [randrange(0,10), randrange(0,10), randrange(0,10), randrange(0,10), randrange(0,10), randrange(0,10), randrange(0,10)]
        MasterVariableContainer.master.slaves[str(id)] = [ip, port, time()]
        return jsonify({"id": str(id)})
