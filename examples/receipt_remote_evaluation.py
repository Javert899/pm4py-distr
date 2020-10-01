import inspect
import os
import sys
import unittest
import time
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from pm4pydistr.remote_wrapper import factory as wrapper_factory
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.algo.discovery.inductive import algorithm as inductive_miner

# possibility to limit the number of sublogs (per slave) that are considered
max_no_samples = 5
# create the wrapper
wrapper = wrapper_factory.apply("137.226.117.71", "7001", "hello", "receipt", parameters={"no_samples": max_no_samples})

log = xes_importer.apply("../tests/receipt.xes")
net, im, fm = inductive_miner.apply(log)

print(wrapper.calculate_fitness_with_tbr(net, im, fm, log))
#print(wrapper.calculate_fitness_with_alignments(net, im, fm, log, parameters={"align_variant": "dijkstra_less_memory"}))
print(wrapper.calculate_fitness_with_alignments(net, im, fm, log, parameters={"align_variant": "recomp_maximal"}))
print(wrapper.calculate_precision_with_tbr(net, im, fm, log))
