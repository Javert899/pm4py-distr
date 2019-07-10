from threading import Thread
import time
from pm4pydistr.configuration import SLEEPING_TIME, SESSION_EXPIRATION


class SessionChecker(Thread):
    def __init__(self, master):
        self.master = master
        Thread.__init__(self)

    def run(self):
        while True:
            time.sleep(SLEEPING_TIME)

            for slave in self.master.slaves:
                slave_time = self.master.slaves[slave][-1]

                if (time.time() - slave_time) > SESSION_EXPIRATION:
                    #print("expired slave ", slave, self.master.slaves[slave])
                    del self.master.slaves[slave]
                    if len(self.master.slaves) > 0:
                        self.master.do_assignment()
                        self.master.make_slaves_load()
