from pm4pydistr.local_wrapper import factory as wrapper_factory

wrapper = wrapper_factory.apply("../master/receipt")
cm = wrapper.correlation_miner()
