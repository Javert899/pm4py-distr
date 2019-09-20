from pm4pydistr.remote_wrapper.distr_log_obj import DistrLogObj
from pm4pydistr.configuration import PARAMETER_USE_TRANSITION, DEFAULT_USE_TRANSITION
from pm4pydistr.configuration import PARAMETER_NO_SAMPLES, DEFAULT_MAX_NO_SAMPLES
from pm4pydistr.configuration import PARAMETER_NUM_RET_ITEMS
import requests
import json
import time
from pm4py.util import constants


class ClassicDistrLogObject(DistrLogObj):
    def __init__(self, hostname, port, keyphrase, log_name, parameters=None):
        DistrLogObj.__init__(self, hostname, port, keyphrase, log_name, parameters=parameters)
        self.check_connession()

    def check_connession(self):
        url = self.get_url("getLoadingStatus")
        max_retry_conn = 13
        sleep_time = 0.1
        connected = False
        for i in range(max_retry_conn):
            try:
                r = requests.get(url)
                content = json.loads(r.text)
                if content["keyphrase_correct"]:
                    if content["first_loading_done"]:
                        if not content["log_assignment_done"]:
                            url2 = self.get_url("doLogAssignment")
                            print(time.time(), "doing initial log assignment into slaves")
                            r2 = requests.get(url2)
                            print(time.time(), "done initial log assignment into slaves (they are working now :) )")
                        connected = True
                    else:
                        print(time.time(), "services still loading")
                else:
                    print(time.time(), "password uncorrect!")
                    break
            except:
                print(time.time(), "connection with host failed (%d out of %d)" % (i+1, max_retry_conn))
                if i+1 == max_retry_conn:
                    break
                sleep_time = sleep_time * 1.5
                time.sleep(sleep_time)

        if not connected:
            raise Exception("impossible to set up a connection with the specified host!! launching exception")

        while True:
            r = requests.get(url)
            content = json.loads(r.text)
            if content["slave_loading_requested"] and content["finished_slaves"] > 0 and content["finished_slaves"] == content["slaves_count"]:
                break
            else:
                print(time.time(), "slaves still coming up, waiting a little more! finished log assignation for %d slaves out of %d" % (content["finished_slaves"], content["slaves_count"]))
            sleep_time = sleep_time * 1.5
            time.sleep(sleep_time)

    def get_url(self, service, parameters=None):
        if parameters is None:
            parameters = {}

        for key in self.init_parameters:
            if key not in parameters:
                parameters[key] = self.init_parameters[key]

        use_transition = parameters[PARAMETER_USE_TRANSITION] if PARAMETER_USE_TRANSITION in parameters else DEFAULT_USE_TRANSITION
        no_samples = parameters[PARAMETER_NO_SAMPLES] if PARAMETER_NO_SAMPLES in parameters else DEFAULT_MAX_NO_SAMPLES

        stru = "http://" + self.hostname + ":" + str(
            self.port) + "/" + service + "?keyphrase=" + self.keyphrase + "&process=" + self.log_name + "&session=" + str(
            self.session) + "&use_transition="+str(use_transition) + "&no_samples=" + str(no_samples)

        if constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY in parameters:
            stru = stru + "&attribute_key=" + str(parameters[constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY])

        if "performance_required" in parameters:
            stru = stru + "&performance_required=" + str(parameters["performance_required"])

        if PARAMETER_NUM_RET_ITEMS in parameters:
            stru = stru + "&"+PARAMETER_NUM_RET_ITEMS+"="+str(parameters[PARAMETER_NUM_RET_ITEMS])

        return stru

    def do_log_assignment(self, parameters=None):
        url = self.get_url("doLogAssignment", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        return ret_text

    def add_filter(self, filter_name, filter_value, parameters=None):
        self.filters.append([filter_name, filter_value])
        url = self.get_url("setFilters", parameters=parameters)
        r = requests.post(url, json={"filters": json.dumps(self.filters)})
        return r.text

    def reset_filters(self, parameters=None):
        self.filters = None
        self.filters = []
        url = self.get_url("setFilters", parameters=parameters)
        r = requests.post(url, json={"filters": json.dumps(self.filters)})
        return r.text

    def calculate_dfg(self, parameters=None):
        url = self.get_url("calculateDfg", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        dfg = ret_json["dfg"]
        new_dfg = {}
        for el in dfg:
            new_dfg[(el.split("@@")[0], el.split("@@")[1])] = dfg[el]
        return new_dfg

    def calculate_performance_dfg(self, parameters=None):
        url = self.get_url("calculatePerformanceDfg", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        dfg = ret_json["dfg"]
        new_dfg = {}
        for el in dfg:
            new_dfg[(el.split("@@")[0], el.split("@@")[1])] = dfg[el]
        return new_dfg

    def calculate_composite_object(self, parameters=None):
        url = self.get_url("calculateCompositeObj", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        obj = ret_json["obj"]
        new_frequency = {}
        for el in obj["frequency_dfg"]:
            new_frequency[(el.split("@@")[0], el.split("@@")[1])] = obj["frequency_dfg"][el]
        obj["frequency_dfg"] = new_frequency
        if "performance_dfg" in obj:
            new_performance = {}
            for el in obj["performance_dfg"]:
                new_performance[(el.split("@@")[0], el.split("@@")[1])] = obj["performance_dfg"][el]
            obj["performance_dfg"] = new_performance
        return obj

    def get_end_activities(self, parameters=None):
        url = self.get_url("getEndActivities", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["end_activities"]

    def get_start_activities(self, parameters=None):
        url = self.get_url("getStartActivities", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["start_activities"]

    def get_log_summary(self, parameters=None):
        url = self.get_url("getLogSummary", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["summary"]

    def get_attribute_values(self, attribute_key, parameters=None):
        url = self.get_url("getAttributeValues", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["values"]

    def get_attribute_names(self, parameters=None):
        url = self.get_url("getAttributesNames", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["names"]

    def get_variants(self, parameters=None):
        url = self.get_url("getVariants", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json

    def get_cases(self, parameters=None):
        url = self.get_url("getCases", parameters=parameters)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json

    def get_events(self, case_id, parameters=None):
        url = self.get_url("getEvents", parameters=parameters)
        url = url + "&case_id="+str(case_id)
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return ret_json["events"]

    def get_logs_list(self):
        url = self.get_url("getSublogsId")
        r = requests.get(url)
        ret_text = r.text
        ret_json = json.loads(ret_text)
        return sorted(list(ret_json["sublogs_id"].keys()))
