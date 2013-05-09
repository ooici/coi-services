#!/usr/bin/env python

"""Process performing system profiling"""

__author__ = 'Michael Meisinger'

import datetime
import gevent
import os.path
import pprint
import time
try:
    import psutil
except ImportError as ie:
    print "psutil is not available"

from pyon.public import log, iex, StandaloneProcess, RT

from ooi.timer import Timer,Accumulator

stats = Accumulator(persist=True)


class ContainerProfiler(StandaloneProcess):

    def on_init(self):
        self.profile_interval = float(self.CFG.get_safe("process.containerprofiler.profile_interval", 60.0))

        self.profile_persist = bool(self.CFG.get_safe("process.containerprofiler.profile_persist", True))

        self.profile_filename = self.CFG.get_safe("process.containerprofiler.profile_filename", None)
        if not self.profile_filename:
            dtstr = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
            self.profile_filename = self.profile_filename or "interface/cont_stats_%s.csv" % dtstr

    def on_start(self):
        self.quit_event = gevent.event.Event()
        self.quit_event.clear()

        self._process.thread_manager.spawn(self._profiler_loop, profile_interval=self.profile_interval)

    def on_quit(self):
        self.quit_event.set()

    def _profiler_loop(self, profile_interval):
        pass
        log.debug("Starting ContainerProfiler loop worker: %s interval=%s" , self.id,  profile_interval)

        while not self.quit_event.wait(timeout=profile_interval):
            try:
                profile = ContainerProfiler.get_profile()
                new_stats = []

                for k, v in profile.iteritems():
                    try:
                        v = float(v)
                        new_stats.append(("cont_stats.%s" % k, v))
                    except TypeError as te:
                        if isinstance(v, object):
                            for k1, v1 in v.__dict__.iteritems():
                                new_stats.append(("cont_stats.%s.%s" % (k, k1), float(getattr(v, k1))))
                        elif hasattr(v, "__iter__"):
                            for i, v1 in enumerate(v):
                                new_stats.append(("cont_stats.%s.%s" % (k, i), float(v1)))
                for statl, statv in new_stats:
                    stats.add_value(statl, statv)

                new_stats.sort(key=lambda (k,v): k)

                if self.profile_persist:
                    is_new = not os.path.exists(self.profile_filename)

                    with open(self.profile_filename, "a") as f:
                        if is_new:
                            headers = ",".join([l for (l,v) in new_stats])
                            f.write("time,")
                            f.write(headers)
                            f.write("\n")
                        values = ",".join([str(v) for (l,v) in new_stats])
                        f.write("%s," % time.time())
                        f.write(values)
                        f.write("\n")
                else:
                    log.debug("Container stats: %s", new_stats)
                    #pprint.pprint(profile)

            except Exception as ex:
                log.exception("Unexpected exception during profiling")

    @classmethod
    def get_profile(cls):
        profile = dict(
            cpu_times = psutil.cpu_times(),
            cpu_percent = psutil.cpu_percent(interval=0.1),
            virtual_memory = psutil.virtual_memory(),
            swap_memory = psutil.swap_memory(),
        )
        return profile
