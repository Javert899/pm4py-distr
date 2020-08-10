from pm4pydistr.remote_wrapper import factory as wrapper_factory
from pm4py.visualization.dfg import visualizer as dfg_visualizer

wrapper = wrapper_factory.apply("127.0.0.1", "5001", "hello", "receipt")
dfg, performance_dfg, activities_counter = wrapper.correlation_miner()
gviz = dfg_visualizer.apply(dfg, activities_count=activities_counter, parameters={"format": "svg"})
dfg_visualizer.view(gviz)
