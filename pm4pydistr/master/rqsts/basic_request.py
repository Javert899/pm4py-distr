from threading import Thread


class BasicMasterRequest(Thread):
    def __init__(self, session, target_host, target_port, use_transition, no_samples, content):
        Thread.__init__(self)

    def run(self):
        pass
