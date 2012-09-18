#!/usr/bin/env python

"""
@package ion.agents.platform.platform_agent_launcher
@file    ion/agents/platform/platform_agent_launcher.py
@author  Carlos Rueda
@brief   Helper for launching platform agent processes
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from pyon.event.event import EventSubscriber

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, ProcessStateEnum

from ion.agents.platform.exceptions import PlatformException

from gevent import queue


PA_MOD = 'ion.agents.platform.platform_agent'
PA_CLS = 'PlatformAgent'


# TODO clean up log-and-throw anti-idiom in several places, which is used
# because the exception alone does not show up in the logs!


class Launcher(object):
    """
    Helper for launching platform agent processes.
    """

    def __init__(self):
        self._pd_client = ProcessDispatcherServiceClient()
        self._event_queue = None
        self._event_sub = None

    def launch(self, platform_id, agent_config, timeout_spawn=30):
        """
        Launches a sub-platform agent.

        @param platform_id      Platform ID
        @param agent_config     Agent configuration
        @param timeout_spawn    Timeout in secs for the SPAWN event (by
                                default 30). If None or zero, no wait is performed.

        @retval process ID
        """
        log.debug("launch: platform_id=%s, timeout_spawn=%s",
                  platform_id, str(timeout_spawn))

        try:
            return self._do_launch(platform_id, agent_config, timeout_spawn)
        finally:
            self._event_queue = None
            self._event_sub = None

    def _do_launch(self, platform_id, agent_config, timeout_spawn):

        pa_name = 'PlatformAgent_%s' % platform_id

        pdef = ProcessDefinition(name=pa_name)
        pdef.executable = {
            'module': PA_MOD,
            'class': PA_CLS
        }
        pdef_id = self._pd_client.create_process_definition(process_definition=pdef)

        pid = self._pd_client.create_process(process_definition_id=pdef_id)

        if timeout_spawn:
            self._event_queue = queue.Queue()
            self._subscribe_events(pid)

        log.debug("calling schedule_process: pid=%s", str(pid))

        self._pd_client.schedule_process(process_definition_id=pdef_id,
                                         process_id=pid,
                                         configuration=agent_config)

        if timeout_spawn:
            self._await_state_event(pid, ProcessStateEnum.SPAWN, timeout=timeout_spawn)

        return pid

    def _state_event_callback(self, event, *args, **kwargs):
        state_str = ProcessStateEnum._str_map.get(event.state)
        origin = event.origin
        log.debug("_state_event_callback CALLED: state=%s from %s\n "
                  "event=%s\n args=%s\n kwargs=%s",
            state_str, origin, str(event), str(args), str(kwargs))

        self._event_queue.put(event)

    def _subscribe_events(self, origin):
        self._event_sub = EventSubscriber(
            event_type="ProcessLifecycleEvent",
            callback=self._state_event_callback,
            origin=origin,
            origin_type="DispatchedProcess"
        )
        self._event_sub.start()

        log.debug("_subscribe_events: origin=%s STARTED", str(origin))

    def _await_state_event(self, pid, state, timeout):
        state_str = ProcessStateEnum._str_map.get(state)
        log.debug("_await_state_event: state=%s from %s timeout=%s",
            state_str, str(pid), timeout)

        #check on the process as it exists right now
        process_obj = self._pd_client.read_process(pid)
        log.debug("process_obj.process_state: %s",
                  ProcessStateEnum._str_map.get(process_obj.process_state))

        if state == process_obj.process_state:
            self._event_sub.stop()
            log.debug("ALREADY in state %s", state_str)
            return

        try:
            event = self._event_queue.get(timeout=timeout)
        except queue.Empty:
            msg = "Event timeout! Waited %s seconds for process %s to notifiy state %s" % (
                            timeout, pid, state_str)
            log.error(msg, exc_info=True)
            raise PlatformException(msg)
        except:
            msg = "Something unexpected happened"
            log.error(msg, exc_info=True)
            raise PlatformException(msg)

        log.debug("Got event: %s", event)
        if event.state != state:
            msg = "Expecting state %s but got %s" % (state, event.state)
            log.error(msg)
            raise PlatformException(msg)
        if event.origin != pid:
            msg = "Expecting origin %s but got %s" % (pid, event.origin)
            log.error(msg)
            raise PlatformException(msg)

    def cancel_process(self, pid):
        """
        Helper to terminate a process
        """
        self._pd_client.cancel_process(pid)
