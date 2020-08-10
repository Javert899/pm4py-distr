from pm4pydistr.remote_wrapper import factory as wrapper_factory

wrapper = wrapper_factory.apply("127.0.0.1", "5001", "hello", "receipt")
model = wrapper.discover_skeleton(parameters={"min_var_freq": 10})
conf = wrapper.conformance_skeleton(model)
print(conf)
