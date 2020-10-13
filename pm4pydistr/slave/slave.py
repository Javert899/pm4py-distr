from pm4pydistr.configuration import PARAMETERS_PORT, PARAMETERS_HOST, PARAMETERS_MASTER_HOST, PARAMETERS_MASTER_PORT, \
    PARAMETERS_CONF, BASE_FOLDER_LIST_OPTIONS, PARAMETERS_AUTO_HOST, PARAMETERS_AUTO_PORT

from pm4pydistr.slave.slave_service import SlaveSocketListener
from pm4pydistr.slave.slave_requests import SlaveRequests
from pathlib import Path
from pm4pydistr.util.parquet_importer import importer as parquet_importer
from pm4pydistr.slave.do_ms_ping import DoMasterPing
import uuid
import socket
from contextlib import closing

import os
import shutil

import time

from pm4py.algo.conformance.alignments.variants import dijkstra_no_heuristics, state_equation_a_star, \
    dijkstra_less_memory, state_equation_less_memory
from pm4py.algo.conformance.decomp_alignments.variants import recompos_maximal
from pm4py.algo.conformance.tokenreplay.variants import token_replay


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class Slave:
    def __init__(self, parameters):
        self.parameters = parameters
        self.host = parameters[PARAMETERS_HOST]
        self.port = str(parameters[PARAMETERS_PORT])
        self.master_host = parameters[PARAMETERS_MASTER_HOST]
        self.master_port = str(parameters[PARAMETERS_MASTER_PORT])
        self.conf = parameters[PARAMETERS_CONF]
        if PARAMETERS_AUTO_HOST in parameters and parameters[PARAMETERS_AUTO_HOST] == "1":
            self.conf = str(uuid.uuid4())
            self.host = str(socket.gethostname())
        if PARAMETERS_AUTO_PORT in parameters and parameters[PARAMETERS_AUTO_PORT] == "1":
            self.port = str(find_free_port())
        self.id = None
        self.ping_module = None

        self.filters = {}

        if not os.path.exists(self.conf):
            os.mkdir(self.conf)

        # sleep a while before taking the slaves up :)
        time.sleep(2)

        self.slave_requests = SlaveRequests(self, self.host, self.port, self.master_host, self.master_port, self.conf)

        self.service = SlaveSocketListener(self, self.host, self.port, self.master_host, self.master_port, self.conf)
        self.service.start()

        # sleep a while before taking the slaves up :)
        time.sleep(2)

        self.slave_requests.register_to_webservice()

    def create_folder(self, folder_name):
        # print("create folder " + str(folder_name))
        if not os.path.isdir(os.path.join(self.conf, folder_name)):
            os.mkdir(os.path.join(self.conf, folder_name))

    def load_log(self, folder_name, log_name):
        # print("loading log " + str(log_name)+" into "+str(folder_name))
        if not os.path.exists(os.path.join(self.conf, folder_name, log_name)):
            for folder in BASE_FOLDER_LIST_OPTIONS:
                if folder_name in os.listdir(folder):
                    list_paths = parquet_importer.get_list_parquet(os.path.join(folder, folder_name))
                    list_paths_corr = {}
                    for x in list_paths:
                        list_paths_corr[Path(x).name] = x
                    if log_name in list_paths_corr:
                        # print("log_name",log_name," in ",os.path.join(folder, folder_name),list_paths_corr[log_name])
                        shutil.copyfile(list_paths_corr[log_name], os.path.join(self.conf, folder_name, log_name))

    def enable_ping_of_master(self):
        self.ping_module = DoMasterPing(self, self.conf, self.id, self.master_host, self.master_port)
        self.ping_module.start()


def perform_alignments(petri_string, var_list, parameters=None):
    if parameters is None:
        parameters = {}

    variant = parameters["align_variant"] if "align_variant" in parameters else "state_equation_less_memory"
    parameters["ret_tuple_as_trans_desc"] = True

    if variant == "dijkstra_no_heuristics":
        return dijkstra_no_heuristics.apply_from_variants_list_petri_string(var_list, petri_string,
                                                                            parameters=parameters)
    elif variant == "state_equation_a_star":
        return state_equation_a_star.apply_from_variants_list_petri_string(var_list, petri_string,
                                                                           parameters=parameters)
    elif variant == "dijkstra_less_memory":
        return dijkstra_less_memory.apply_from_variants_list_petri_string(var_list, petri_string, parameters=parameters)
    elif variant == "recomp_maximal":
        return recompos_maximal.apply_from_variants_list_petri_string(var_list, petri_string, parameters=parameters)
    elif variant == "state_equation_less_memory":
        return state_equation_less_memory.apply_from_variants_list_petri_string(var_list, petri_string,
                                                                                parameters=parameters)
    elif variant == "tree_approximated":
        if "classic_alignments_variant" in parameters:
            if parameters["classic_alignments_variant"] == "dijkstra_no_heuristics":
                parameters["classic_alignments_variant"] = dijkstra_no_heuristics
            elif parameters["classic_alignments_variant"] == "dijkstra_less_memory":
                parameters["classic_alignments_variant"] = dijkstra_less_memory
            elif parameters["classic_alignments_variant"] == "recomp_maximal":
                parameters["classic_alignments_variant"] = recompos_maximal
            elif parameters["classic_alignments_variant"] == "state_equation_less_memory":
                parameters["classic_alignments_variant"] = state_equation_less_memory
            elif parameters["classic_alignments_variant"] == "state_equation_a_star":
                parameters["classic_alignments_variant"] = state_equation_a_star
        from pm4py.objects.conversion.process_tree import converter
        if parameters["petri_conversion_version"] == "to_petri_net":
            parameters["petri_conversion_version"] = converter.Variants.TO_PETRI_NET
        elif parameters["petri_conversion_version"] == "to_petri_net_transition_bordered":
            parameters["petri_conversion_version"] = converter.Variants.TO_PETRI_NET_TRANSITION_BORDERED
        tree_align_variant = parameters["tree_align_variant"]
        if tree_align_variant == "matrix_lp":
            from pm4py.algo.conformance.tree_alignments.variants.approximated import matrix_lp as approx_alignments

        else:
            from pm4py.algo.conformance.tree_alignments.variants.approximated import original as approx_alignments

        alignments = approx_alignments.apply_from_variants_tree_string(var_list, petri_string,
                                                                       parameters=parameters)
        return alignments


def perform_token_replay(petri_string, var_list, parameters=None):
    if parameters is None:
        parameters = {}

    enable_parameters_precision = parameters[
        "enable_parameters_precision"] if "enable_parameters_precision" in parameters else False
    consider_remaining_in_fitness = parameters[
        "consider_remaining_in_fitness"] if "consider_remaining_in_fitness" in parameters else True

    parameters["return_names"] = True

    if enable_parameters_precision:
        parameters["consider_remaining_in_fitness"] = False
        parameters["try_to_reach_final_marking_through_hidden"] = False
        parameters["walk_through_hidden_trans"] = True
        parameters["stop_immediately_unfit"] = True
        parameters["consider_remaining_in_fitness"] = consider_remaining_in_fitness

    return token_replay.apply_variants_list_petri_string(var_list, petri_string, parameters=parameters)
