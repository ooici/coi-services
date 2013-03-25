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
from ion.util.agent_launcher import AgentLauncher


class Launcher(object):
    """
    Helper for launching platform and instrument agent processes.
    """

    def __init__(self):
        self._pd_client = ProcessDispatcherServiceClient()
        self._agent_launcher = AgentLauncher(self._pd_client)

    def destroy(self):
        if self._pd_client:
            self._pd_client.close()
            self._pd_client = None
            self._agent_launcher = None

    def launch_platform(self, agt_id, agent_config, timeout_spawn=30):
        """
        Launches a platform agent.

        @param agt_id           Some ID mainly used for logging
        @param agent_config     Agent configuration
        @param timeout_spawn    Timeout in secs for the SPAWN event (by
                                default 30). If None or zero, no wait is performed.

        @return process ID
        """
        log.debug("launch platform: agt_id=%r, timeout_spawn=%s", agt_id, timeout_spawn)

        name = 'PlatformAgent_%s' % agt_id
        pdef = ProcessDefinition(name=name)
        pdef.executable = {
            'module': 'ion.agents.platform.platform_agent',
            'class':  'PlatformAgent'
        }

        pdef_id = self._pd_client.create_process_definition(process_definition=pdef)

        pid = self._agent_launcher.launch(agent_config, pdef_id)

        if timeout_spawn:
            self._agent_launcher.await_launch(timeout_spawn)

        return pid

    def launch_instrument(self, agt_id, agent_config, timeout_spawn=30):
        """
        Launches an instrument agent.

        @param agt_id           Some ID mainly used for logging
        @param agent_config     Agent configuration
        @param timeout_spawn    Timeout in secs for the SPAWN event (by
                                default 30). If None or zero, no wait is performed.

        @return process ID
        """
        log.debug("launch instrument: agt_id=%r, timeout_spawn=%s", agt_id, timeout_spawn)

        name = 'InstrumentAgent_%s' % agt_id
        pdef = ProcessDefinition(name=name)
        pdef.executable = {
            'module': 'ion.agents.instrument.instrument_agent',
            'class':  'InstrumentAgent'
        }

        pdef_id = self._pd_client.create_process_definition(process_definition=pdef)

        pid = self._agent_launcher.launch(agent_config, pdef_id)

        if timeout_spawn:
            self._agent_launcher.await_launch(timeout_spawn)

        return pid

    def cancel_process(self, pid):
        """
        Helper to terminate a process
        """
        self._pd_client.cancel_process(pid)
