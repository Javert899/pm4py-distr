from pm4pydistr.remote_wrapper import factory as wrapper_factory

wrapper = wrapper_factory.apply("127.0.0.1", "5001", "hello", "receipt")
tree = wrapper.get_imd_tree_from_dfg()
alignments = wrapper.perform_alignments_tree_variants(tree)
print(alignments)
