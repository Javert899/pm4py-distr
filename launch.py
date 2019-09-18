import sys
from pm4pydistr.master.master import Master
from pm4pydistr.slave.slave import Slave
from pm4pydistr.configuration import PARAMETERS_TYPE, PARAMETERS_PORT, PARAMETERS_MASTER_HOST, PARAMETERS_MASTER_PORT, \
    PARAMETERS_CONF, PARAMETERS_HOST, ENVIRON_PREFIX
from pm4pydistr.configuration import PORT, MASTER_HOST, MASTER_PORT, CONF, THIS_HOST
import os


parameters = {}

parameters[PARAMETERS_PORT] = PORT
parameters[PARAMETERS_MASTER_HOST] = MASTER_HOST
parameters[PARAMETERS_MASTER_PORT] = MASTER_PORT
parameters[PARAMETERS_CONF] = CONF
parameters[PARAMETERS_HOST] = THIS_HOST

i = 1
while i < len(sys.argv):
    if sys.argv[i] == PARAMETERS_TYPE:
        parameters[sys.argv[i]] = sys.argv[i + 1]
    elif sys.argv[i] == PARAMETERS_HOST:
        parameters[sys.argv[i]] = sys.argv[i + 1]
    elif sys.argv[i] == PARAMETERS_PORT:
        parameters[sys.argv[i]] = sys.argv[i + 1]
    elif sys.argv[i] == PARAMETERS_MASTER_HOST:
        parameters[sys.argv[i]] = sys.argv[i + 1]
    elif sys.argv[i] == PARAMETERS_MASTER_PORT:
        parameters[sys.argv[i]] = sys.argv[i + 1]
    elif sys.argv[i] == PARAMETERS_CONF:
        parameters[sys.argv[i]] = sys.argv[i + 1]

    i = i + 1

if ENVIRON_PREFIX+PARAMETERS_CONF in os.environ:
    parameters[PARAMETERS_CONF] = os.environ[ENVIRON_PREFIX+PARAMETERS_CONF]
if ENVIRON_PREFIX+PARAMETERS_HOST in os.environ:
    parameters[PARAMETERS_HOST] = os.environ[ENVIRON_PREFIX+PARAMETERS_HOST]
if ENVIRON_PREFIX+PARAMETERS_PORT in os.environ:
    parameters[PARAMETERS_PORT] = os.environ[ENVIRON_PREFIX+PARAMETERS_PORT]
if ENVIRON_PREFIX+PARAMETERS_TYPE in os.environ:
    parameters[PARAMETERS_TYPE] = os.environ[ENVIRON_PREFIX+PARAMETERS_TYPE]
if ENVIRON_PREFIX+PARAMETERS_MASTER_HOST in os.environ:
    parameters[PARAMETERS_MASTER_HOST] = os.environ[ENVIRON_PREFIX+PARAMETERS_MASTER_HOST]
if ENVIRON_PREFIX+PARAMETERS_MASTER_PORT in os.environ:
    parameters[PARAMETERS_MASTER_PORT] = os.environ[ENVIRON_PREFIX+PARAMETERS_MASTER_PORT]

if parameters[PARAMETERS_TYPE] == "master":
    m = Master(parameters)
else:
    m = Slave(parameters)

print(parameters)
