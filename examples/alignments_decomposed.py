from pm4pydistr.remote_wrapper import factory as wrapper_factory
from pm4py.evaluation.replay_fitness.versions import alignment_based
import time

wrapper = wrapper_factory.apply("137.226.117.71", "7001", "hello", "receipt")
net, im, fm = wrapper.get_imd_net_im_fm_from_dfg()
aa = time.time()
alignments = wrapper.perform_alignments_net_variants(net, im, fm,
                                                     parameters={"align_variant": "recomp_maximal",
                                                                 "max_align_time_trace": 10.0, "max_align_time": 100.0,
                                                                 "max_thread_join_time": 150.0})
alignments = [y for x, y in alignments.items()]
bb = time.time()
fitness = alignment_based.evaluate(alignments)
print(bb - aa)
print(fitness)
print("total traces = ", len(alignments))
print("successful alignments = ", len([x for x in alignments if x is not None]))
# print(alignments)
