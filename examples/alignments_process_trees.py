from pm4pydistr.remote_wrapper import factory as wrapper_factory
import time
from pm4py.evaluation.replay_fitness.versions import alignment_based
from pm4py.visualization.process_tree import visualizer as pt_visualizer

wrapper = wrapper_factory.apply("137.226.117.71", "7001", "hello", "roadtraffic")
tree = wrapper.get_imd_tree_from_dfg()
gviz = pt_visualizer.apply(tree, parameters={"format": "svg"})
pt_visualizer.view(gviz)
aa = time.time()
alignments = wrapper.perform_alignments_tree_variants(tree)
align_list = [y for x,y in alignments.items()]
bb = time.time()
print(alignments)
print(bb-aa)
print(alignment_based.evaluate(align_list))
