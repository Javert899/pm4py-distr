import uuid

class DistrLogObj(object):
    def __init__(self, hostname, port, keyphrase, log_name, parameters=None):
        if parameters is None:
            parameters = {}
        self.hostname = hostname
        self.port = port
        self.keyphrase = keyphrase
        self.log_name = log_name
        self.init_parameters = parameters
        self.session = str(uuid.uuid4())
        self.filters = []

    def add_filter(self, filter_name, filter_value, parameters=None):
        raise Exception("not implemented")

    def reset_filters(self, parameters=None):
        raise Exception("not implemented")

    def set_filters(self, parameters=None):
        raise Exception("not implemented")

    def calculate_dfg(self, parameters=None):
        raise Exception("not implemented")

    def calculate_performance_dfg(self, parameters=None):
        raise Exception("not implemented")

    def calculate_composite_object(self, parameters=None):
        raise Exception("not implemented")

    def get_end_activities(self, parameters=None):
        raise Exception("not implemented")

    def get_start_activities(self, parameters=None):
        raise Exception("not implemented")

    def do_log_assignment(self, parameters=None):
        raise Exception("not implemented")

    def get_log_summary(self, parameters=None):
        raise Exception("not implemented")

    def get_attribute_values(self, attribute_key, parameters=None):
        raise Exception("not implemented")

    def get_attribute_names(self, parameters=None):
        raise Exception("not implemented")

    def get_variants(self, parameters=None):
        raise Exception("not implemented")

    def get_cases(self, parameters=None):
        raise Exception("not implemented")

    def get_events(self, case_id, parameters=None):
        raise Exception("not implemented")

    def get_logs_list(self):
        raise Exception("not implemented")

    def get_events_per_dotted(self, attribute1, attribute2, attribute3, parameters=None):
        raise Exception("not implemented")

    def get_events_per_time(self, parameters=None):
        raise Exception("not implemented")

    def get_events_per_time_first(self, parameters=None):
        raise Exception("not implemented")

    def get_events_per_case(self, parameters=None):
        raise Exception("not implemented")

    def get_case_duration(self, parameters=None):
        raise Exception("not implemented")

    def get_numeric_attribute(self, attribute_key, parameters=None):
        raise Exception("not implemented")

    def do_caching(self, parameters=None):
        raise Exception("not implemented")

    def perform_alignments_net_log(self, net, im, fm, log, parameters=None):
        raise Exception("not implemented")

    def perform_alignments_net_variants(self, net, im, fm, var_list=None, parameters=None):
        raise Exception("not implemented")

    def perform_tbr_net_log(self, net, im, fm, log, parameters=None):
        raise Exception("not implemented")

    def perform_tbr_net_variants(self, net, im, fm, var_list=None, parameters=None):
        raise Exception("not implemented")

    def calculate_fitness_with_alignments(self, net, im, fm, log, parameters=None):
        raise Exception("not implemented")

    def calculate_precision_with_tbr(self, net, im, fm, log, parameters=None):
        raise Exception("not implemented")

    def calculate_fitness_with_tbr(self, net, im, fm, log, parameters=None):
        raise Exception("not implemented")

    def get_distr_log_footprints(self, parameters=None):
        raise Exception("not implemented")

    def get_imd_tree_from_dfg(self, parameters=None):
        raise Exception("not implemented")

    def get_imd_net_im_fm_from_dfg(self, parameters=None):
        raise Exception("not implemented")

    def get_im_tree_from_variants(self, parameters=None):
        raise Exception("not implemented")

    def get_im_net_im_fm_from_variants(self, parameters=None):
        raise Exception("not implemented")

    def perform_alignments_tree_variants(self, tree, var_list=None, parameters=None):
        raise Exception("not implemented")

    def perform_alignments_tree_log(self,tree, log, parameters=None):
        raise Exception("not implemented")

    def discover_skeleton(self, parameters=None):
        raise Exception("not implemented")

    def conformance_skeleton(self, model, parameters=None):
        raise Exception("not implemented")

    def correlation_miner(self, parameters=None):
        raise Exception("not implemented")
