from pm4pydistr.remote_wrapper import factory as wrapper_factory

wrapper = wrapper_factory.apply("137.226.117.71", "7001", "hello", "receipt")
fp = wrapper.get_distr_log_footprints()
print(fp)
