#!/usr/bin/env python

"""
@package ion.agents.platform.test.test_mission_manager
@file    ion/agents/platform/test/test_mission_manager.py
@author  Carlos Rueda
@brief   Test cases for platform agent integrated with mission scheduler
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_simple_mission_command_state
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_simple_mission_streaming_state
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_multiple_missions
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_simple_mission_ports_on_off
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_mission_abort
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_mission_multiple_instruments
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_simple_event_driven_mission

from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform
from ion.agents.platform.platform_agent_enums import PlatformAgentEvent
from ion.agents.platform.platform_agent_enums import PlatformAgentState
from ion.agents.platform.util.network_util import NetworkUtil

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
    def _get_network_definition_filename(self):
        return 'ion/agents/platform/test/platform-network-1.yml'

    def _run_startup_commands(self, recursion=True):
        self._ping_agent()
        self._initialize(recursion)
        self._go_active(recursion)
        self._run(recursion)

    def _go_inactive(self, recursion=True):
        """
        Temporary more permissive _go_inactive while we handle more internal
        coordination with instruments.
        """
        kwargs = dict(recursion=recursion)
        cmd = AgentCommand(command=PlatformAgentEvent.GO_INACTIVE, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        state = self._get_state()
        if state == PlatformAgentState.INACTIVE:
            return retval
        else:
            log.warn("Expecting INACTIVE but got: %s", state)

    def _run_shutdown_commands(self, recursion=True):
        """
        Issues commands as needed to bring the parent platform to shutdown.
        """
        log.debug('[mm] _run_shutdown_commands.  state=%s', self._get_state())
        try:
            state = self._get_state()
            if state != PlatformAgentState.UNINITIALIZED:
                if state in [PlatformAgentState.IDLE,
                             PlatformAgentState.STOPPED,
                             PlatformAgentState.COMMAND,
                             PlatformAgentState.LOST_CONNECTION]:
                    self._go_inactive(recursion)

                self._reset(recursion)
        finally:  # attempt shutdown anyway
            self._shutdown(True)  # NOTE: shutdown always with recursion=True

    def _run_mission(self, mission_id, mission_yml):
        kwargs = dict(mission_id=mission_id, mission_yml=mission_yml)
        cmd = AgentCommand(command=PlatformAgentEvent.RUN_MISSION, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        log.debug('[mm] _run_mission mission_id=%s: RUN_MISSION return: %s', mission_id, retval)

    def _await_mission_completion(self, mission_state, max_wait=None):
        """
        @param mission_state  The mission that we are waiting to leave
        @param max_wait       maximum wait; no effect if None
        """
        step = 5
        elapsed = 0
        while (max_wait is None or elapsed < max_wait) and mission_state == self._get_state():
            sleep(step)
            elapsed += step
            if elapsed % 20 == 0:
                log.debug('[mm] _await_mission_completion: waiting, elapsed=%s', elapsed)

        state = self._get_state()
        if mission_state != state:
            log.info('[mm] _await_mission_completion: completed, elapsed=%s, '
                     'transitioned from=%s to=%s', elapsed, mission_state, state)
        else:
            log.warn('[mm] _await_mission_completion: timeout, elapsed=%s, '
                     'still in state=%s', elapsed, mission_state)

    def _test_simple_mission(self, instr_keys, mission_filename, in_command_state, max_wait=None):
        """
        Verifies mission execution, mainly as coordinated from platform agent
        and with some verifications related with expected mission event
        publications.

        @param instr_keys
                    Instruments to associate with parent platform; these
                    should be ones referenced in the mission plan.
        @param mission_filename
        @param in_command_state
                    True to start mission execution in COMMAND state.
                    False to start mission execution in MONITORING state.
        @param max_wait
                    maximum wait for mission completion; no effect if None.
                    Actual argument in the tests is based on local tests plus
                    some extra time mainly for buildbot. The extra time is rather
                    large due to the high variability in the execution elapsed
                    time. In any case, we want to avoid waiting forever.
        """
        self._set_receive_timeout()

        if in_command_state:
            base_state    = PlatformAgentState.COMMAND
            mission_state = PlatformAgentState.MISSION_COMMAND
        else:
            base_state    = PlatformAgentState.MONITORING
            mission_state = PlatformAgentState.MISSION_STREAMING

        # start everything up to platform agent in COMMAND state.
        p_root = self._set_up_single_platform_with_some_instruments(instr_keys)
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)
        self._run_startup_commands()

        if not in_command_state:
            self._start_resource_monitoring()

        # now prepare and run mission:

        # TODO determine appropriate instrument identification mechanism as the
        # instrument keys (like SBE37_SIM_02) are basically only known in
        # the scope of the tests. In the following, we transform the mission
        # file so the instrument keys are replaced by the corresponding
        # instrument_device_id's:

        with open(mission_filename) as f:
            mission_yml = f.read()
        for instr_key in instr_keys:
            i_obj = self._get_instrument(instr_key)
            resource_id = i_obj.instrument_device_id
            log.debug('[mm] replacing %s to %s', instr_key, resource_id)
            mission_yml = mission_yml.replace(instr_key, resource_id)
        log.debug('[mm] mission_yml=%s', mission_yml)

        # prepare to receive expected mission events:
        async_event_result, events_received = self._start_event_subscriber2(
            count=1,
            event_type="MissionLifecycleEvent",
            origin_type="PlatformDevice"
        )
        log.info('[mm] mission event subscriber started')

        # now run mission:
        mission_id = mission_filename
        self._run_mission(mission_id, mission_yml)

        state = self._get_state()
        if state == mission_state:
            # ok, this is the general expected behaviour here as typical
            # mission plans should at least take several seconds to complete;
            # now wait until mission is completed:
            self._await_mission_completion(mission_state, max_wait)
        # else: mission completed/failed very quickly; we should be
        # back in the base state, as verified below in general.

        # verify we are back to the base_state:
        self._assert_state(base_state)

        # verify reception of event:
        # NOTE: for initial test at the moment just expect at least one such event.
        # TODO but there are more that should be verified (started, stopped...)
        async_event_result.get(timeout=self._receive_timeout)
        self.assertGreaterEqual(len(events_received), 1)
        log.info('[mm] mission events received: (%d): %s', len(events_received), events_received)

        if not in_command_state:
            self._stop_resource_monitoring()

    def _test_multiple_missions(self, instr_keys, mission_filenames, max_wait=None):
        """
        Verifies platform agent can dispatch execution of multiple missions.
        No explicit verifications in the test, but the logs should show lines
        like the following where the number of running missions is included:

        DEBUG Dummy-204 ion.agents.platform.mission_manager:58 [mm] starting mission_id='ion/agents/platform/test/multi_mission_1.yml' (#running missions=1)
        ...
        DEBUG Dummy-205 ion.agents.platform.mission_manager:58 [mm] starting mission_id='ion/agents/platform/test/multi_mission_2.yml' (#running missions=2)

        @param instr_keys
                    Instruments to associate with parent platform; these
                    should be ones references in the mission plans.
        @param mission_filenames
                    List of filenames
        @param max_wait
                    maximum wait for mission completion; no effect if None.
        """
        self._set_receive_timeout()

        base_state    = PlatformAgentState.COMMAND
        mission_state = PlatformAgentState.MISSION_COMMAND

        # start everything up to platform agent in COMMAND state.
        # Instruments launched here are the ones referenced in the mission
        # file below.
        p_root = self._set_up_single_platform_with_some_instruments(instr_keys)
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)
        self._run_startup_commands()

        for mission_filename in mission_filenames:
            with open(mission_filename) as f:
                mission_yml = f.read()
            for instr_key in instr_keys:
                i_obj = self._get_instrument(instr_key)
                resource_id = i_obj.instrument_device_id
                log.debug('[mm] replacing %s to %s', instr_key, resource_id)
                mission_yml = mission_yml.replace(instr_key, resource_id)
            log.debug('[mm] mission_yml=%s', mission_yml)
            self._run_mission(mission_filename, mission_yml)

        state = self._get_state()
        if state == mission_state:
            self._await_mission_completion(mission_state, max_wait)

        # verify we are back to the base_state:
        self._assert_state(base_state)

    def test_simple_mission_command_state(self):
        #
        # With mission plan to be started in COMMAND state.
        #
        self._test_simple_mission(
            ['SBE37_SIM_02'],
            "ion/agents/platform/test/mission_RSN_simulator0C.yml",
            in_command_state=True,
            max_wait=200 + 300)

    def test_simple_mission_streaming_state(self):
        #
        # With mission plan to be started in MONITORING state.
        #
        self._test_simple_mission(
            ['SBE37_SIM_02'],
            "ion/agents/platform/test/mission_RSN_simulator0S.yml",
            in_command_state=False,
            max_wait=200 + 300)

    def test_multiple_missions(self):
        #
        # Verifies the PA can execute multiple missions plans concurrently.
        #
        self._test_multiple_missions(
            ['SBE37_SIM_02', 'SBE37_SIM_03'],
            ["ion/agents/platform/test/multi_mission_1.yml",
             "ion/agents/platform/test/multi_mission_2.yml"],
            max_wait=200 + 300)

    def test_simple_mission_ports_on_off(self):
        #
        # Test TURN_ON_PORT and TURN_OFF_PORT
        # Mission plan to be started in COMMAND state.
        #
        self._test_simple_mission(
            ['SBE37_SIM_02'],
            "ion/agents/platform/test/mission_RSN_simulator_ports.yml",
            in_command_state=True,
            max_wait=200 + 300)

    def test_mission_abort(self):
        #
        # Intentially invalid mission file
        # Mission plan to be started in COMMAND state.
        #
        self._test_simple_mission(
            ['SBE37_SIM_02'],
            "ion/agents/platform/test/mission_RSN_simulator_abort.yml",
            in_command_state=True,
            max_wait=200 + 300)

    def test_mission_multiple_instruments(self):
        #
        # Multiple instruments example
        # Mission plan to be started in COMMAND state.
        #
        self._test_simple_mission(
            ['SBE37_SIM_02', 'SBE37_SIM_03'],
            "ion/agents/platform/test/mission_RSN_simulator_multiple_threads.yml",
            in_command_state=True,
            max_wait=200 + 300)

    def test_simple_event_driven_mission(self):
        #
        # Event driven mission example
        # Mission plan to be started in COMMAND state.
        #
        self._test_simple_mission(
            ['SBE37_SIM_02', 'SBE37_SIM_03'],
            "ion/agents/platform/test/mission_RSN_simulator_event.yml",
            in_command_state=True,
            max_wait=200 + 300)
