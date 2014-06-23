#!/usr/bin/env python

"""Additional tests focused on robustness and destructive testing"""

__author__ = 'Carlos Rueda'

# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_robustness.py:TestPlatformRobustness.test_instrument_reset_externally
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_robustness.py:TestPlatformRobustness.test_adverse_activation_sequence

from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform, instruments_dict
from pyon.public import log, CFG
from pyon.util.context import LocalContextMixin
from pyon.agent.agent import ResourceAgentState, ResourceAgentEvent
from interface.objects import AgentCommand

from mock import patch
import unittest
import os


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
@unittest.skipIf((not os.getenv('PYCC_MODE', False)) and os.getenv('CEI_LAUNCH_TEST', False), 'Skip until tests support launch port agent configurations.')
class TestPlatformRobustness(BaseIntTestPlatform):

    def _launch_network(self, p_root, recursion=True):
        def shutdown():
            try:
                self._go_inactive(recursion)
                self._reset(recursion)
            finally:  # attempt shutdown anyway
                self._shutdown(True)  # NOTE: shutdown always with recursion=True

        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(shutdown)

    def test_instrument_reset_externally(self):
        #
        # - a network of one platform with 2 instruments is launched and put in command state
        # - one of the instruments is directly reset here simulating some external situation
        # - network is shutdown following regular sequence
        # - test should complete fine: the already reset child instrument should not
        #   cause issue during the reset sequence from parent platform agent.
        #
        # During the shutdown sequence the following should be logged out by the platform prior to trying
        # the GO_INACTIVE command to the (already reset) instrument:
        # ion.agents.platform.platform_agent:2195 'LJ01D': instrument '8761366c8d734a4ba886606c3a47b621':
        #       current_state='RESOURCE_AGENT_STATE_UNINITIALIZED'
        #       expected_state='RESOURCE_AGENT_STATE_INACTIVE'
        #       command='RESOURCE_AGENT_EVENT_GO_INACTIVE', comp=-1, actl=decr, acceptable_state=True
        #
        self._set_receive_timeout()
        recursion = True

        instr_keys = ['SBE37_SIM_01', 'SBE37_SIM_02']
        p_root = self._set_up_single_platform_with_some_instruments(instr_keys)
        self._launch_network(p_root, recursion)

        i_obj = self._get_instrument(instr_keys[0])
        ia_client = self._create_resource_agent_client(i_obj.instrument_device_id)

        # command sequence:
        self._ping_agent()
        self._initialize(recursion)
        self._go_active(recursion)
        self._run(recursion)

        self.assertEqual(ia_client.get_agent_state(), ResourceAgentState.COMMAND)

        # reset the instrument
        log.debug("resetting instrument %r", instr_keys[0])
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = ia_client.execute_agent(cmd, timeout=self._receive_timeout)
        log.debug("reset instrument %r returned: %s", instr_keys[0], retval)
        self.assertEqual(ia_client.get_agent_state(), ResourceAgentState.UNINITIALIZED)

        # and let the test complete with regular shutdown sequence.

    def test_adverse_activation_sequence(self):
        #
        # - initialize network (so all agents get INITIALIZED)
        # - externally reset an instrument (so that instrument gets to UNINITIALIZED)
        # - activate network (so a "device_failed_command" event should be published because
        #   instrument is not INITIALIZED)
        # - verify publication of the "device_failed_command" event
        # - similarly, "run" the network
        # - verify publication of 2nd "device_failed_command" event
        #
        # note: the platform successfully completes the GO_ACTIVE and RUN commands even with the failed child;
        # see https://confluence.oceanobservatories.org/display/CIDev/Platform+commands
        #
        self._set_receive_timeout()
        recursion = True

        instr_keys = ['SBE37_SIM_01']
        p_root = self._set_up_single_platform_with_some_instruments(instr_keys)
        self._launch_network(p_root, recursion)

        i_obj = self._get_instrument(instr_keys[0])
        ia_client = self._create_resource_agent_client(i_obj.instrument_device_id)

        # initialize the network
        self._ping_agent()
        self._initialize(recursion)

        self.assertEqual(ia_client.get_agent_state(), ResourceAgentState.INACTIVE)

        # reset the instrument before we continue the activation of the network
        log.debug("resetting instrument %r", instr_keys[0])
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = ia_client.execute_agent(cmd, timeout=self._receive_timeout)
        log.debug("reset instrument %r returned: %s", instr_keys[0], retval)
        self.assertEqual(ia_client.get_agent_state(), ResourceAgentState.UNINITIALIZED)

        # prepare to receive "device_failed_command" event during GO_ACTIVE:
        # (See StatusManager.publish_device_failed_command_event)
        async_event_result, events_received = self._start_event_subscriber2(
            count=1,
            event_type="DeviceStatusEvent",
            origin_type="PlatformDevice",
            sub_type="device_failed_command"
        )

        # activate network:
        self._go_active(recursion)

        # verify publication of "device_failed_command" event
        async_event_result.get(timeout=self._receive_timeout)
        self.assertEquals(len(events_received), 1)
        event_received = events_received[0]
        log.info("DeviceStatusEvents received (%d): %s", len(events_received), event_received)
        self.assertGreaterEqual(len(event_received.values), 1)
        failed_resource_id = event_received.values[0]
        self.assertEquals(i_obj.instrument_device_id, failed_resource_id)

        # "run" network:
        self._run(recursion)

        # verify publication of 2nd "device_failed_command" event
        async_event_result.get(timeout=self._receive_timeout)
        self.assertEquals(len(events_received), 2)
        event_received = events_received[1]
        log.info("DeviceStatusEvents received (%d): %s", len(events_received), event_received)
        self.assertGreaterEqual(len(event_received.values), 1)
        failed_resource_id = event_received.values[0]
        self.assertEquals(i_obj.instrument_device_id, failed_resource_id)
