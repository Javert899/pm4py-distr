import inspect
import os
import sys
import unittest
import time
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from pm4pydistr.remote_wrapper import factory as wrapper_factory
from pm4py.algo.discovery.inductive import factory as inductive_miner

# possibility to limit the number of sublogs (per slave) that are considered
max_no_samples = 5
# create the wrapper
wrapper = wrapper_factory.apply("137.226.117.71", "5001", "hello", "receipt", parameters={"no_samples": max_no_samples})
# gets the log summary (number of cases and number of events)
print(wrapper.get_log_summary())
dfg = wrapper.calculate_dfg()
net, im, fm = inductive_miner.apply_dfg(dfg)
tbr_result = wrapper.perform_tbr_net_variants(net, im, fm, var_list=None)
aa = time.time()
#aligned_traces = wrapper.perform_alignments_net_variants(net, im, fm, var_list=None, parameters={"align_variant": "dijkstra_less_memory"})
aligned_traces = wrapper.perform_alignments_net_variants(net, im, fm, var_list=None, parameters={"align_variant": "recomp_maximal"})
bb = time.time()
print(aligned_traces)
print(bb-aa)
