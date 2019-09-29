from pm4pydistr.local_wrapper import factory as wrapper_factory

# possibility to limit the number of sublogs (per slave) that are considered
max_no_samples = 1
# create the wrapper
wrapper = wrapper_factory.apply("../master/roadtraffic", parameters={"no_samples": max_no_samples})
# gets the log summary (number of cases and number of events)
print(wrapper.get_log_summary())
# gets all the attributes of the log
print(wrapper.get_attribute_names())
# gets all the values of the concept:name attribute along with the count
print(wrapper.get_attribute_values("concept:name"))
# gets the start and end activities of the log
print(wrapper.get_start_activities())
print(wrapper.get_end_activities())
# gets the path of the log (frequency DFG) along with their count
print(wrapper.calculate_dfg())
# add a filter on the presence of the "Send Fine" activity
wrapper.add_filter("attributes_pos_trace", ["concept:name", ["Send Fine"]])
# see if the number of events changes
print(wrapper.get_log_summary())

# add a filter on cases going from "Send Fine" to "Payment"
# wrapper.add_filter("paths_pos_trace", ["concept:name", ["Send Fine@@Payment"]])
# see if the number of events changes
# print(wrapper.get_log_summary())

# add a filter on cases ending with "Payment" or "Add penalty"
wrapper.add_filter("end_activities", ["Payment", "Add penalty"])
# see if the number of events changes
print(wrapper.get_log_summary())

# gets the case duration graph points
x, y = wrapper.get_case_duration()
#print(x, y)
# add a filter on cases lasting from 1M to 1Y
wrapper.add_filter("case_performance_filter", str(86400*30)+"@@@"+str(86400*365))
# see if the number of events changes
print(wrapper.get_log_summary())

# gets the events per time
x, y = wrapper.get_events_per_time()
# print(x, y)
# apply a filter on cases contained in the interval [09-2014, 01-2008]
wrapper.add_filter("timestamp_trace_containing", "1100000000@@@1200000000")
# see if the number of events changes
print(wrapper.get_log_summary())

# gets a graph representing the values of the numeric attribute "amount"
x, y = wrapper.get_numeric_attribute("amount")