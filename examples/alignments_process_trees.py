from pm4pydistr.remote_wrapper import factory as wrapper_factory
from pm4py.objects.process_tree.importer import importer as pt_importer
from pm4py.evaluation.replay_fitness.versions import alignment_based
import time

wrapper = wrapper_factory.apply("137.226.117.71", "7001", "hello", "receipt")
tree = pt_importer.apply('../tests/receipt.ptml')
# tree = wrapper.get_imd_tree_from_dfg()
aa = time.time()
alignments = wrapper.perform_alignments_tree_variants(tree,
                                                      parameters={"max_align_time_trace": 10.0, "max_align_time": 100.0,
                                                                  "max_thread_join_time": 150.0})
alignments = [y for x, y in alignments.items()]
bb = time.time()
# print(alignments)
fitness = alignment_based.evaluate(alignments)
print(bb - aa)
print(fitness)
print("total traces = ", len(alignments))
print("successful alignments = ", len([x for x in alignments if x is not None]))
# print(alignments)
