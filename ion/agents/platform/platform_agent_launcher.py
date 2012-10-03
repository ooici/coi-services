#!/usr/bin/env python

"""
@package ion.agents.platform.platform_agent_launcher
@file    ion/agents/platform/platform_agent_launcher.py
@author  Carlos Rueda
@brief   Helper for launching platform agent processes. This helper was introduced
         to facilitate testing and diagnosing of launching issues in coi_pycc and
         other builds in buildbot, and also to facilitate other variations
         like direct PlatformAgent instantation ("standalone" mode) for
         testing purposes.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from pyon.event.event import EventSubscriber
from pyon.util.containers import DotDict

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, ProcessStateEnum

from ion.services.cei.process_dispatcher_service import ProcessStateGate

from ion.agents.platform.exceptions import PlatformException

from gevent import queue


PA_MOD = 'ion.agents.platform.platform_agent'
PA_CLS = 'PlatformAgent'


# TODO clean up log-and-throw anti-idiom in several places, which is used
# because the exception alone does not show up in the logs!


class LauncherFactory(object):
    """
    Convenient factory of launcher objects.
    """

    @classmethod
    def createLauncher(cls, use_gate=True, standalone=None):
        if standalone:
            return _StandaloneLauncher(standalone)
        else:
            return _Launcher(use_gate)


class _Launcher(object):
    """
    Helper for launching platform agent processes. This helper was introduced
    to facilitate testing and diagnosing of launching issues in coi_pycc and
    other builds in buildbot.
    """

    def __init__(self, use_gate=True):
        """
        @param use_gate True (the default) to use ProcessStateGate pattern.
                        Otherwise, the create_process/schedule_process/_await_state_event
                        pattern (as described in
                        https://confluence.oceanobservatories.org/display/CIDev/R2+Process+Dispatcher+Guide
                        as of Sept 14/12) is used.
        """
        self._use_gate = use_gate
        self._pd_client = ProcessDispatcherServiceClient()

    def launch(self, platform_id, agent_config, timeout_spawn=30):
        """
        Launches a sub-platform agent.

        @param platform_id      Platform ID
        @param agent_config     Agent configuration
        @param timeout_spawn    Timeout in secs for the SPAWN event (by
                                default 30). If None or zero, no wait is performed.

        @retval process ID
        """
        log.debug("launch: platform_id=%r, timeout_spawn=%s",
                  platform_id, str(timeout_spawn))

        if self._use_gate:
            return self._do_launch_gate(platform_id, agent_config, timeout_spawn)

        try:
            return self._do_launch(platform_id, agent_config, timeout_spawn)
        finally:
            self._event_queue = None
            self._event_sub = None

    def cancel_process(self, pid):
        """
        Helper to terminate a process
        """
        self._pd_client.cancel_process(pid)

    def _do_launch_gate(self, platform_id, agent_config, timeout_spawn):
        """
        The method for when using the ProcessStateGate pattern, which is the
        one used by test_oms_launch to launch the root platform.
        """
        log.debug("_do_launch_gate: platform_id=%r", platform_id)

        pa_name = 'PlatformAgent_%s' % platform_id

        pdef = ProcessDefinition(name=pa_name)
        pdef.executable = {
            'module': PA_MOD,
            'class': PA_CLS
        }
        pdef_id = self._pd_client.create_process_definition(process_definition=pdef)

        log.debug("using schedule_process directly %r", platform_id)

        pid = self._pd_client.schedule_process(process_definition_id=pdef_id,
                                         schedule=None,
                                         configuration=agent_config)

        if timeout_spawn:
            # ProcessStateGate used as indicated in its pydoc (9/21/12)
            gate = ProcessStateGate(self._pd_client.read_process, pid, ProcessStateEnum.SPAWN)
            err_msg = None
            try:
                if not gate.await(timeout_spawn):
                    err_msg = "The platform agent instance did not spawn in %s seconds" %\
                          timeout_spawn
                    log.error(err_msg)
            except Exception as e:
                log.error("Exception while waiting for platform agent instance "
                          "(platform_id=%r) "
                          "to spawn in %s seconds: %s",
                          platform_id, timeout_spawn, str(e)) #,exc_Info=True)
            if err_msg:
                raise PlatformException(err_msg)

        log.debug("_do_launch_gate: platform agent spawned, platform_id=%s, pid=%r "
                  "(ProcessStateGate pattern used)",
                  platform_id, pid)

        return pid

    def _do_launch(self, platform_id, agent_config, timeout_spawn):
        """
        The method for the
        "create_process/subscribe-to-event/schedule_process/_await_state_event"
        pattern as described in
        https://confluence.oceanobservatories.org/display/CIDev/R2+Process+Dispatcher+Guide
        as of Sept 14/12.
        """

        log.debug("launching plaform agent, platform_id=%r "
                  "('create_process/subscribe-to-event/schedule_process/_await_state_event' pattern used)",
                  platform_id)

        self._event_queue = None
        self._event_sub = None

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

        log.debug("Plaform agent spawned, platform_id=%r, pid=%r "
                  "('create_process/subscribe-to-event/schedule_process/_await_state_event' pattern used)",
                  platform_id, pid)

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
            log.error(msg) #, exc_info=True)
            raise PlatformException(msg)
        except Exception as e:
            msg = "Something unexpected happened: %s" % str(e)
            log.error(msg) #, exc_info=True)
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


class _StandaloneLauncher(object):
    """
    Direct builder of PlatformAgent instances.
    """
    def __init__(self, standalone):
        self._standalone = standalone

    def launch(self, platform_id, agent_config, timeout_spawn=30):
        from ion.agents.platform.platform_agent import PlatformAgent

        standalone = dict((k, v) for k, v in self._standalone.iteritems())
        standalone['platform_id'] = platform_id

        pa = PlatformAgent(standalone=standalone)
        CFG = pa.CFG = DotDict()
        CFG.agent = agent_config['agent']
        CFG.stream_config = agent_config['stream_config']

        return pa

    def cancel_process(self, pid):
        # nothing needs to be done.
        pass
