#!/usr/bin/env python

"""
@package ion.agents.platform.test.test_mission_manager
@file    ion/agents/platform/test/test_mission_manager.py
@author  Carlos Rueda
@brief   Test cases for platform agent integrated with mission scheduler
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

# bin/nosetests -sv ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_simple_mission_command_state
# bin/nosetests -sv ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_simple_mission_streaming_state


from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform
from ion.agents.platform.platform_agent_enums import PlatformAgentEvent
from ion.agents.platform.platform_agent_enums import PlatformAgentState

from interface.objects import AgentCommand

from pyon.public import log, CFG

from gevent import sleep
from mock import patch
from unittest import skipIf
import os


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
@skipIf((not os.getenv('PYCC_MODE', False)) and os.getenv('CEI_LAUNCH_TEST', False), 'Skip until tests support launch port agent configurations.')
class TestPlatformAgentMission(BaseIntTestPlatform):
    """
    """
    def _run_startup_commands(self, recursion=True):
        self._ping_agent()
        self._initialize(recursion)
        self._go_active(recursion)
        self._run(recursion)

    def _run_shutdown_commands(self, recursion=True):
        try:
            self._go_inactive(recursion)
            self._reset(recursion)
        finally:  # attempt shutdown anyway
            self._shutdown(True)  # NOTE: shutdown always with recursion=True

    def _set_mission(self, yaml_filename):
        log.debug('_set_mission: setting agent param mission = %s', yaml_filename)
        self._pa_client.set_agent({'mission': yaml_filename})

    def _get_mission(self):
        mission = self._pa_client.get_agent(['mission'])['mission']
        self.assertIsNotNone(mission)
        log.debug('_get_mission: agent param mission = %s', mission)
        return mission

    def _run_mission(self):
        cmd = AgentCommand(command=PlatformAgentEvent.RUN_MISSION)
        retval = self._execute_agent(cmd)
        log.debug('_run_mission: RUN_MISSION return: %s', retval)

    def _await_mission_completion(self, state, max_wait):
        step = 5
        elapsed = 0
        while elapsed < max_wait and state == self._get_state():
            log.debug('_await_mission_completion: elapsed=%s', elapsed)
            sleep(step)
            elapsed += step

        if state != self._get_state():
            log.info('_await_mission_completion: left state=%s, new state=%s',
                     state, self._get_state())
        else:
            log.warn('_await_mission_completion: still in state=%s', state)

    def test_simple_mission_command_state(self):
        #
        # Mission execution is started in COMMAND state.
        # Verifies mission execution involving a platform with 2 instruments
        # as specified in mission_RSN_simulator1_finite_loop.yml.
        #
        self._set_receive_timeout()

        # start everything up to platform agent in COMMAND state.
        # Instruments launched here are the ones referenced in the mission.
        p_root = self._set_up_single_platform_with_some_instruments(
            ['SBE37_SIM_02', 'SBE37_SIM_03'])
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)
        self._run_startup_commands()

        # now set and run mission:
        filename = "ion/agents/platform/test/mission_RSN_simulator1_finite_loop.yml"
        self._set_mission(filename)
        self._run_mission()
        self._assert_state(PlatformAgentState.MISSION_COMMAND)

        self._await_mission_completion(PlatformAgentState.MISSION_COMMAND, 240)

        # verify we are back to COMMAND state:
        self._assert_state(PlatformAgentState.COMMAND)

    def test_simple_mission_streaming_state(self):
        #
        # Mission execution is started in MONITORING state.
        # Verifies mission execution involving a platform with 2 instruments
        # as specified in mission_RSN_simulator1_finite_loop.yml.
        #
        self._set_receive_timeout()

        # start everything up to platform agent in MONITORING state.
        # Instruments launched here are the ones referenced in the mission.
        p_root = self._set_up_single_platform_with_some_instruments(
            ['SBE37_SIM_02', 'SBE37_SIM_03'])
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)
        self._run_startup_commands()

        self._start_resource_monitoring()

        # now set and run mission:
        filename = "ion/agents/platform/test/mission_RSN_simulator1_finite_loop.yml"
        self._set_mission(filename)
        self._run_mission()
        self._assert_state(PlatformAgentState.MISSION_STREAMING)

        self._await_mission_completion(PlatformAgentState.MISSION_STREAMING, 240)

        # verify we are back to MONITORING state:
        self._assert_state(PlatformAgentState.MONITORING)

        self._stop_resource_monitoring()
