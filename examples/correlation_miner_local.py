from pm4pydistr.local_wrapper import factory as wrapper_factory

wrapper = wrapper_factory.apply("../master/receipt")
dfg, performance_dfg = wrapper.correlation_miner()
print(dfg)
print(performance_dfg)
