#!/usr/bin/env python

"""
@package ion.agents.platform.test.test_mission_manager
@file    ion/agents/platform/test/test_mission_manager.py
@author  Carlos Rueda
@brief   Test cases for platform agent integrated with mission scheduler
"""

__author__ = 'Carlos Rueda'


# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_simple_mission_command_state
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_simple_mission_streaming_state
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_multiple_missions
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_simple_mission_ports_on_off
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_mission_abort
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_mission_multiple_instruments
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_simple_event_driven_mission
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_manager.py:TestPlatformAgentMission.test_mock_shallow_profiler

from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform
from ion.agents.platform.platform_agent_enums import PlatformAgentEvent
from ion.agents.platform.platform_agent_enums import PlatformAgentState

from interface.objects import AgentCommand
from interface.objects import MissionExecutionStatus

from pyon.event.event import EventPublisher
from pyon.public import log, CFG

import gevent
import logging
import time
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

    def _get_processed_yml(self, instr_keys, mission_filename):
        with open(mission_filename) as f:
            mission_yml = f.read()
        log.debug('[mm] mission_yml=%s', mission_yml)
        return mission_yml

    def _start_everything_up(self, instr_keys, up_to_command_state):
        p_root = self._set_up_single_platform_with_some_instruments(instr_keys)
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)
        self._run_startup_commands()

        if not up_to_command_state:
            self._start_resource_monitoring()

        return p_root

    def simulate_profiler_events(self, profile_type):
        """
        Simulate the Shallow Water profiler stair step mission
        """
        event_publisher = EventPublisher(event_type="OMSDeviceStatusEvent")
        num_profiles = 1

        def profiler_event_state_change(state, sleep_duration):
            """
            Publish the state change event

            @param state            the state entered by the driver.
            @param sleep_duration   seconds to sleep for after event
            """
            # Create event publisher.
            event_data = {'sub_type': state}
            event_publisher.publish_event(event_type='OMSDeviceStatusEvent',
                                          origin='LJ01D',
                                          **event_data)
            gevent.sleep(sleep_duration)

        def stair_step_simulator():
            # Let's simulate a profiler stair step scenario

            seconds_between_steps = 30
            seconds_at_ceiling = 5
            num_steps = 1

            # Start Mission
            profiler_event_state_change('StartMission', 1)
            # Going up
            profiler_event_state_change('StartingAscent', seconds_between_steps)

            for x in range(num_profiles):

                # Step up
                for down in range(num_steps):
                    profiler_event_state_change('atStep', seconds_between_steps)
                    profiler_event_state_change('StartingUp', 1)

                # Ascend to ceiling
                profiler_event_state_change('atCeiling', seconds_at_ceiling )
                # Start to descend
                profiler_event_state_change('StartingDescent', seconds_between_steps)
                # Arrive at floor
                profiler_event_state_change('atFloor', 1)

            profiler_event_state_change('MissionComplete', 1)

        def up_down_simulator():
            # Let's simulate a profiler up-down scenario
            seconds_between_steps = 5 * 60
            # Start Mission
            profiler_event_state_change('StartMission', 1)
            # Start ascent
            profiler_event_state_change('StartingAscent', seconds_between_steps)

            for x in range(num_profiles):
                # Ascend to ceiling
                profiler_event_state_change('atCeiling', seconds_between_steps)
                # Start to descend
                profiler_event_state_change('StartingDescent', seconds_between_steps)
                # Arrive at floor
                profiler_event_state_change('atFloor', seconds_between_steps)

            profiler_event_state_change('MissionComplete', 1)

        def simulator_error():
            # Let's simulate a profiler up-down scenario
            seconds_between_steps = 60
            # Start Mission
            profiler_event_state_change('StartMission', 1)
            # Start ascent
            profiler_event_state_change('StartingAscent', seconds_between_steps)
            # Ascend to ceiling
            profiler_event_state_change('atCeiling', seconds_between_steps)
            profiler_event_state_change('systemError', 1)

        if profile_type == 'stair_step':
            stair_step_simulator()
        else:
            up_down_simulator()

    def _test_mission(self, instr_keys, mission_filename, in_command_state,
                      expected_events, max_wait):
        """
        Verifies mission execution, mainly as coordinated from platform agent
        and with some verifications related with expected mission event
        publications.

        @param instr_keys
                    Instruments to associate with parent platform; these
                    should be ones referenced in the mission plan.
        @param mission_filename
                    The mission plan
        @param in_command_state
                    True to start mission execution in COMMAND state.
                    False to start mission execution in MONITORING state.
        @param expected_events
                    Can be a number or a list of dicts:
                        [{'sub_type': x, 'mission_thread_id': x}, ...]
        @param max_wait
                    Max wait to receive the expected events
        """
        self._set_receive_timeout()

        p_root = self._start_everything_up(instr_keys, in_command_state)
        mission_yml = self._get_processed_yml(instr_keys, mission_filename)

        # prepare to receive expected mission events:
        no_expected_events = len(expected_events) if isinstance(expected_events, list) else expected_events
        async_event_result, events_received = self._start_event_subscriber2(
            count=no_expected_events,
            event_type="MissionLifecycleEvent",
            cb=lambda evt, *args, **kwargs: log.debug('MissionLifecycleEvent received: %s', evt),
            origin_type="PlatformDevice",
            origin=p_root.platform_device_id)

        # prepare to receive the two state transition events
        #  - when transitioning to mission state
        #  - when transitioning back to base state
        origin = p_root.platform_device_id
        trans_async_event_result, trans_events_received = self._start_event_subscriber2(
            count=2,
            event_type="ResourceAgentStateEvent",
            cb=lambda evt, *args, **kwargs: log.debug('ResourceAgentStateEvent received: %s', evt),
            origin_type="PlatformDevice",
            origin=origin)

        log.debug('[mm] waiting for %s expected MissionLifecycleEvents from origin=%r', no_expected_events, origin)
        log.debug('[mm] waiting for %s expected ResourceAgentStateEvents from origin=%r', 2, origin)

        self._run_mission(mission_filename, mission_yml)
        try:
            started = time.time()
            async_event_result.get(timeout=max_wait)
            if log.isEnabledFor(logging.DEBUG):
                simplified = [dict(sub_type=e.sub_type,
                                   mission_thread_id=e.mission_thread_id,
                                   execution_status=e.execution_status) for e in events_received]
                log.debug('[mm] got %d MissionLifecycleEvents received (%s secs):\n%s',
                          len(events_received),
                          time.time() - started,
                          self._pp.pformat(simplified))

            self.assertEqual(len(events_received), no_expected_events)

            if isinstance(expected_events, list):
                # verify each expected event was received regardless of order or reception:
                for expected_event in expected_events:
                    was_received = False
                    for idx, received_event in enumerate(events_received):
                        contained = True
                        for key in expected_event.keys():
                            if key not in received_event or received_event[key] != expected_event[key]:
                                contained = False
                                break
                        if contained:
                            was_received = True
                            break
                    self.assertTrue(was_received)

                # # the following would force the order of event reception, which is too strict as the
                # # events might arrive out of order (especially with missions involving multiple instruments)
                # for i, expected_event in enumerate(expected_events):
                #     received_event = {}
                #     for key in expected_event.keys():
                #         self.assertIn(key, events_received[i])
                #         received_event[key] = events_received[i][key]
                #     self.assertEqual(expected_event, received_event)

        finally:
            try:
                trans_async_event_result.get(timeout=max_wait)
                if log.isEnabledFor(logging.DEBUG):
                    log.debug('[mm] got %d ResourceAgentStateEvents:\n%s',
                              len(trans_events_received),
                              self._pp.pformat(trans_events_received))
                self.assertGreaterEqual(len(trans_events_received), 2)
            finally:
                if not in_command_state:
                    self._stop_resource_monitoring()

    def _test_multiple_missions(self, instr_keys, mission_filenames, max_wait=None):
        """
        Verifies platform agent can dispatch execution of multiple missions.
        Should receive 2 ResourceAgentStateEvents from the platform:
         - when transitioning to MISSION_COMMAND (upon first mission execution started)
         - when transitioning back to COMMAND
        No other explicit verifications, but the logs should show lines
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

        # start everything up to platform agent in COMMAND state.
        p_root = self._start_everything_up(instr_keys, True)

        origin = p_root.platform_device_id
        async_event_result, events_received = self._start_event_subscriber2(
            count=2,
            event_type="ResourceAgentStateEvent",
            origin_type="PlatformDevice",
            origin=origin)

        for mission_filename in mission_filenames:
            mission_yml = self._get_processed_yml(instr_keys, mission_filename)
            self._run_mission(mission_filename, mission_yml)

        log.debug('[mm] waiting for %s expected MissionLifecycleEvents from origin=%r', 2, origin)
        started = time.time()
        async_event_result.get(timeout=max_wait)
        if log.isEnabledFor(logging.DEBUG):
            log.debug('[mm] got %d events (%s secs):\n%s',
                      len(events_received),
                      time.time() - started,
                      self._pp.pformat(events_received))
        self.assertEqual(len(events_received), 2)

    def test_simple_mission_command_state(self):
        #
        # With mission plan to be started in COMMAND state.
        # Should receive 6 events from mission executive if successful
        #
        expected_events = [
            {'sub_type': 'STARTING', 'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},  # 'Mission thread _ has started'
            {'sub_type': 'STARTED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},  # 'MissionSequence starting...'
            {'sub_type': 'STOPPED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STOPPED',  'mission_thread_id': '' , 'execution_status': MissionExecutionStatus.OK}]

        self._test_mission(
            ['SBE37_SIM_02'],
            "ion/agents/platform/test/mission_RSN_simulator0C.yml",
            in_command_state=True,
            expected_events=expected_events,
            max_wait=200 + 300)

    def test_simple_mission_streaming_state(self):
        #
        # With mission plan to be started in MONITORING state.
        # Should receive 6 events from mission executive. For this it waits
        # for 3 mins for duration of mission plus some tolerance.
        #
        instr_keys = ['SBE37_SIM_02']
        mission_filename = "ion/agents/platform/test/mission_RSN_simulator0S.yml"

        expected_events = [
            {'sub_type': 'STARTING', 'mission_thread_id': ''},
            {'sub_type': 'STARTED',  'mission_thread_id': ''},
            {'sub_type': 'STARTED',  'mission_thread_id': '0'},  # 'Mission thread 0 has started'
            {'sub_type': 'STARTED',  'mission_thread_id': '0'},  # 'MissionSequence starting...'
            {'sub_type': 'STOPPED',  'mission_thread_id': '0'},
            {'sub_type': 'STOPPED',  'mission_thread_id': ''}]
        max_wait = 180 + 60

        self._test_mission(instr_keys, mission_filename, False, expected_events, max_wait)

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
        # Tests TURN_ON_PORT and TURN_OFF_PORT commands.
        # Mission plan to be started in COMMAND state.
        # Should receive 6 events from mission executive if successful
        #
        expected_events = [
            {'sub_type': 'STARTING', 'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},  # 'Mission thread _ has started'
            {'sub_type': 'STARTED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},  # 'MissionSequence starting...'
            {'sub_type': 'STOPPED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STOPPED',  'mission_thread_id': '' , 'execution_status': MissionExecutionStatus.OK}]

        self._test_mission(
            ['SBE37_SIM_02'],
            "ion/agents/platform/test/mission_RSN_simulator_ports.yml",
            in_command_state=True,
            expected_events=expected_events,
            max_wait=200 + 300)

    def test_mission_abort(self):
        #
        # Intentially invalid mission file
        # Mission plan to be started in COMMAND state.
        # Should receive 6 events from mission executive if successful
        #
        expected_events = [
            {'sub_type': 'STARTING', 'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},  # 'Mission thread _ has started'
            {'sub_type': 'STARTED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},  # 'MissionSequence starting...'
            {'sub_type': 'STOPPED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.FAILED},
            {'sub_type': 'STOPPED',  'mission_thread_id': '',  'execution_status': MissionExecutionStatus.FAILED}]

        self._test_mission(
            ['SBE37_SIM_02'],
            "ion/agents/platform/test/mission_RSN_simulator_abort.yml",
            in_command_state=True,
            expected_events=expected_events,
            max_wait=200 + 300)

    def test_mission_multiple_instruments(self):
        #
        # Multiple instruments example
        # Mission plan to be started in COMMAND state.
        # Should receive 9 events from mission executive if successful
        #
        expected_events = [
            {'sub_type': 'STARTING', 'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},  # 'Mission thread _ has started'
            {'sub_type': 'STARTED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},  # 'MissionSequence starting...'
            {'sub_type': 'STARTED',  'mission_thread_id': '1', 'execution_status': MissionExecutionStatus.OK},  # 'Mission thread _ has started'
            {'sub_type': 'STARTED',  'mission_thread_id': '1', 'execution_status': MissionExecutionStatus.OK},  # 'MissionSequence starting...'
            {'sub_type': 'STOPPED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STOPPED',  'mission_thread_id': '1', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STOPPED',  'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK}]

        self._test_mission(
            ['SBE37_SIM_02', 'SBE37_SIM_03'],
            "ion/agents/platform/test/mission_RSN_simulator_multiple_threads.yml",
            in_command_state=True,
            expected_events=expected_events,
            max_wait=200 + 300)

    def test_simple_event_driven_mission(self):
        #
        # Event driven mission example
        # Mission plan to be started in COMMAND state.
        # Should receive 9 events from mission executive if successful
        #
        expected_events = [
            {'sub_type': 'STARTING', 'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},  # 'Mission thread _ has started'
            {'sub_type': 'STARTED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},  # 'MissionSequence starting...'
            {'sub_type': 'STARTED',  'mission_thread_id': '1', 'execution_status': MissionExecutionStatus.OK},  # 'Mission thread _ has started'
            {'sub_type': 'STARTED',  'mission_thread_id': '1', 'execution_status': MissionExecutionStatus.OK},  # 'MissionSequence starting...'
            {'sub_type': 'STOPPED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STOPPED',  'mission_thread_id': '1', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STOPPED',  'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK}]

        self._test_mission(
            ['SBE37_SIM_02', 'SBE37_SIM_03'],
            "ion/agents/platform/test/mission_RSN_simulator_event.yml",
            in_command_state=True,
            expected_events=expected_events,
            max_wait=200 + 300)

    def test_mock_shallow_profiler(self):
        #
        # Shallow profiler mission example from mock profiler
        # Mission plan to be started in COMMAND state.
        # Should receive 9 events from mission executive if successful
        #
        expected_events = [
            {'sub_type': 'STARTING', 'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '1', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '2', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STOPPED',  'mission_thread_id': '0', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '1', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STOPPED',  'mission_thread_id': '1', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STARTED',  'mission_thread_id': '2', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STOPPED',  'mission_thread_id': '2', 'execution_status': MissionExecutionStatus.OK},
            {'sub_type': 'STOPPED',  'mission_thread_id': '',  'execution_status': MissionExecutionStatus.OK}]

        # Start profiler event simulator and mission scheduler
        threads = []
        threads.append(gevent.spawn(self._test_mission,
                       ['SBE37_SIM_02', 'SBE37_SIM_03', 'SBE37_SIM_04'],
                       "ion/agents/platform/test/mission_ShallowProfiler_simulated.yml",
                       in_command_state=True,
                       expected_events=expected_events,
                       max_wait=200 + 300))

        threads.append(gevent.spawn_later(45, self.simulate_profiler_events, 'stair_step'))

        gevent.joinall(threads)
