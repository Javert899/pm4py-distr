import sys
from pm4pydistr.master.master import Master
from pm4pydistr.slave.slave import Slave
from pm4pydistr import configuration
from pm4pydistr.configuration import PARAMETERS_TYPE, PARAMETERS_PORT, PARAMETERS_MASTER_HOST, PARAMETERS_MASTER_PORT, \
    PARAMETERS_CONF, PARAMETERS_HOST, ENVIRON_PREFIX, PARAMETERS_AUTO_HOST, PARAMETERS_KEYPHRASE, PARAMETERS_BASE_FOLDERS, PARAMETERS_AUTO_PORT
from pm4pydistr.configuration import PORT, MASTER_HOST, MASTER_PORT, CONF, THIS_HOST, DEFAULT_TYPE
import os


parameters = {}

parameters[PARAMETERS_TYPE] = DEFAULT_TYPE
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
    elif sys.argv[i] == PARAMETERS_AUTO_HOST:
        parameters[sys.argv[i]] = sys.argv[i + 1]
    elif sys.argv[i] == PARAMETERS_AUTO_PORT:
        parameters[sys.argv[i]] = sys.argv[i + 1]
    elif sys.argv[i] == PARAMETERS_KEYPHRASE:
        configuration.KEYPHRASE = sys.argv[i + 1]
    elif sys.argv[i] == PARAMETERS_BASE_FOLDERS:
        configuration.BASE_FOLDER_LIST_OPTIONS = sys.argv[i + 1].split("@@")
    i = i + 1

if os.getenv(ENVIRON_PREFIX+PARAMETERS_CONF) is not None:
    parameters[PARAMETERS_CONF] = os.getenv(ENVIRON_PREFIX+PARAMETERS_CONF)
if os.getenv(ENVIRON_PREFIX+PARAMETERS_HOST) is not None:
    parameters[PARAMETERS_HOST] = os.getenv(ENVIRON_PREFIX+PARAMETERS_HOST)
if os.getenv(ENVIRON_PREFIX+PARAMETERS_PORT) is not None:
    parameters[PARAMETERS_PORT] = os.getenv(ENVIRON_PREFIX+PARAMETERS_PORT)
if os.getenv(ENVIRON_PREFIX+PARAMETERS_TYPE) is not None:
    parameters[PARAMETERS_TYPE] = os.getenv(ENVIRON_PREFIX+PARAMETERS_TYPE)
if os.getenv(ENVIRON_PREFIX+PARAMETERS_MASTER_HOST) is not None:
    parameters[PARAMETERS_MASTER_HOST] = os.getenv(ENVIRON_PREFIX+PARAMETERS_MASTER_HOST)
if os.getenv(ENVIRON_PREFIX+PARAMETERS_MASTER_PORT) is not None:
    parameters[PARAMETERS_MASTER_PORT] = os.getenv(ENVIRON_PREFIX+PARAMETERS_MASTER_PORT)

if os.getenv(ENVIRON_PREFIX+PARAMETERS_AUTO_HOST) is not None:
    parameters[PARAMETERS_AUTO_HOST] = os.getenv(ENVIRON_PREFIX+PARAMETERS_AUTO_HOST)
if os.getenv(ENVIRON_PREFIX+PARAMETERS_AUTO_PORT) is not None:
    parameters[PARAMETERS_AUTO_PORT] = os.getenv(ENVIRON_PREFIX+PARAMETERS_AUTO_PORT)
if os.getenv(ENVIRON_PREFIX+PARAMETERS_KEYPHRASE) is not None:
    configuration.KEYPHRASE = os.getenv(ENVIRON_PREFIX+PARAMETERS_KEYPHRASE)
if os.getenv(ENVIRON_PREFIX+PARAMETERS_BASE_FOLDERS) is not None:
    configuration.PARAMETERS_BASE_FOLDERS = os.getenv(ENVIRON_PREFIX+PARAMETERS_BASE_FOLDERS).split("@@")

if parameters[PARAMETERS_TYPE] == "master":
    m = Master(parameters)
else:
    m = Slave(parameters)
