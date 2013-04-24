#!/usr/bin/env python

"""
@package ion.agents.platform.launcher
@file    ion/agents/platform/launcher.py
@author  Carlos Rueda
@brief   Helper for launching platform and instrument agent processes.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition
from interface.objects import ProcessStateEnum
from ion.services.cei.process_dispatcher_service import ProcessStateGate
from ion.util.agent_launcher import AgentLauncher


class Launcher(object):
    """
    Helper for launching platform and instrument agent processes.
    """

    def __init__(self, timeout_spawn):
        """
        @param timeout_spawn    Default timeout in secs for the RUNNING event.
        """
        self._timeout_spawn = timeout_spawn
        self._pd_client = ProcessDispatcherServiceClient()
        self._agent_launcher = AgentLauncher(self._pd_client)

    def destroy(self):
        if self._pd_client:
            self._pd_client.close()
            self._pd_client = None
            self._agent_launcher = None

    def launch_platform(self, agt_id, agent_config, timeout_spawn=None):
        """
        Launches a platform agent.

        @param agt_id           Some ID mainly used for logging
        @param agent_config     Agent configuration
        @param timeout_spawn    Timeout in secs for the RUNNING event (by
                                default, the value given in constructor).
                                If None or zero, no wait is performed.

        @return process ID
        """
        timeout_spawn = timeout_spawn or self._timeout_spawn
        log.debug("launch_platform: agt_id=%r, timeout_spawn=%s", agt_id, timeout_spawn)

        name = 'PlatformAgent_%s' % agt_id
        pdef = ProcessDefinition(name=name)
        pdef.executable = {
            'module': 'ion.agents.platform.platform_agent',
            'class':  'PlatformAgent'
        }

        pdef_id = self._pd_client.create_process_definition(process_definition=pdef)

        pid = self._agent_launcher.launch(agent_config, pdef_id)

        if timeout_spawn:
            log.debug("launch_platform: agt_id=%r: waiting for RUNNING", agt_id)
            self._agent_launcher.await_launch(timeout_spawn)
            log.debug("launch_platform: agt_id=%r: RUNNING", agt_id)

        return pid

    def launch_instrument(self, agt_id, agent_config, timeout_spawn=None):
        """
        Launches an instrument agent.

        @param agt_id           Some ID mainly used for logging
        @param agent_config     Agent configuration
        @param timeout_spawn    Timeout in secs for the RUNNING event (by
                                default, the value given in constructor).
                                If None or zero, no wait is performed.

        @return process ID
        """
        timeout_spawn = timeout_spawn or self._timeout_spawn
        log.debug("launch_instrument: agt_id=%r, timeout_spawn=%s", agt_id, timeout_spawn)

        name = 'InstrumentAgent_%s' % agt_id
        pdef = ProcessDefinition(name=name)
        pdef.executable = {
            'module': 'ion.agents.instrument.instrument_agent',
            'class':  'InstrumentAgent'
        }

        pdef_id = self._pd_client.create_process_definition(process_definition=pdef)

        pid = self._agent_launcher.launch(agent_config, pdef_id)

        if timeout_spawn:
            log.debug("launch_instrument: agt_id=%r: waiting for RUNNING", agt_id)
            self._agent_launcher.await_launch(timeout_spawn)
            log.debug("launch_instrument: agt_id=%r: RUNNING", agt_id)

        return pid

    def cancel_process(self, pid, timeout_cancel=None):
        """
        Helper to terminate a process
        """
        pinfo = self._pd_client.read_process(pid)
        if pinfo.process_state != ProcessStateEnum.RUNNING:
            log.debug("cancel_process: pid=%r is not RUNNING", pid)
            return

        log.debug("cancel_process: canceling pid=%r", pid)
        self._pd_client.cancel_process(pid)

        if timeout_cancel:
            log.debug("waiting %s seconds for preocess to cancel", timeout_cancel)
            psg = ProcessStateGate(self._pd_client.read_process, pid,
                                   ProcessStateEnum.TERMINATED)
            if not psg.await(timeout_cancel):
                log.debug("Process %r failed to get to TERMINATED in %s seconds",
                          pid, timeout_cancel)
