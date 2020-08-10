from pm4pydistr.remote_wrapper import factory as wrapper_factory

wrapper = wrapper_factory.apply("127.0.0.1", "5001", "hello", "receipt")
fp = wrapper.get_distr_log_footprints()
print(fp)
