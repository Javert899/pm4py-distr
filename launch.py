import sys
from pm4pydistr.master.master import Master
from pm4pydistr.slave.slave import Slave
from pm4pydistr.configuration import PARAMETERS_TYPE, PARAMETERS_PORT, PARAMETERS_MASTER_HOST, PARAMETERS_MASTER_PORT, \
    PARAMETERS_CONF, PARAMETERS_HOST
from pm4pydistr.configuration import PORT, MASTER_HOST, MASTER_PORT, CONF, THIS_HOST

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

if parameters[PARAMETERS_TYPE] == "master":
    m = Master(parameters)
else:
    m = Slave(parameters)

print(parameters)
