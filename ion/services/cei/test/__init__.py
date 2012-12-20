from gevent import queue
from datetime import datetime, timedelta

from pyon.event.event import EventSubscriber
from pyon.public import log
from pyon.core.bootstrap import CFG

from interface.objects import ProcessStateEnum

class ProcessStateWaiter(object):
    def __init__(self):
        self.event_queue = queue.Queue()
        self.event_sub = None

    def start(self, process_id=None):
        assert self.event_sub is None
        self.event_sub = EventSubscriber(event_type="ProcessLifecycleEvent",
            callback=self._event_callback, origin=process_id, origin_type="DispatchedProcess")
        self.event_sub.start()

    def stop(self):
        if self.event_sub:
            self.event_sub.stop()
            self.event_sub = None

    def _event_callback(self, event, *args, **kwargs):
        self.event_queue.put(event)

    def await_state_event(self, pid=None, state=None, timeout=30, strict=False):
        """Wait for a state event for a process.

        if strict is False, allow intermediary events
        """

        start_time = datetime.now()

        assert state in ProcessStateEnum._str_map, "process state %s unknown!" % state
        state_str = ProcessStateEnum._str_map.get(state)

        while 1:
            if datetime.now() - start_time > timedelta(seconds=timeout):
                raise AssertionError("Waiter timeout! Waited %s seconds for process %s state %s" % (timeout, pid, state_str))
            try:
                event = self.event_queue.get(timeout=timeout)
            except queue.Empty:
                raise AssertionError("Event timeout! Waited %s seconds for process %s state %s" % (timeout, pid, state_str))
            log.debug("Got event: %s", event)

            if (pid is None or event.origin == pid) and (state is None or event.state == state):
                return event

            elif strict:
                raise AssertionError("Got unexpected event %s. Expected state %s for process %s" % (event, state_str, pid))

def get_dashi_uri_from_cfg(config=None):
    if config is None:
        config = CFG
    rabbit_host = config.get_safe("server.amqp.host")
    rabbit_user = config.get_safe("server.amqp.username")
    rabbit_pass = config.get_safe("server.amqp.password")

    if not (rabbit_host and rabbit_user and rabbit_pass):
        raise Exception("cannot form dashi URI")

    return "amqp://%s:%s@%s/" % (rabbit_user, rabbit_pass,
                                      rabbit_host)
