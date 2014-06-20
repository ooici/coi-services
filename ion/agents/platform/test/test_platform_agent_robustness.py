#!/usr/bin/env python

"""Additional tests focused on robustness and destructive testing"""

__author__ = 'Carlos Rueda'

# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_robustness.py:TestPlatformRobustness.test_instrument_reset_externally

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
        def start():
            self._ping_agent()
            self._initialize(recursion)
            self._go_active(recursion)
            self._run(recursion)

        def shutdown():
            try:
                self._go_inactive(recursion)
                self._reset(recursion)
            finally:  # attempt shutdown anyway
                self._shutdown(True)  # NOTE: shutdown always with recursion=True

        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(shutdown)
        log.info('launching network')
        start()

    def test_instrument_reset_externally(self):
        #
        # A network of one platform with 2 instruments is launched and put in command state;
        # then one of the instruments is directly reset here simulating some external situation;
        # then the network is shutdown following regular sequence;
        # The test should complete fine: the already reset child instrument should not
        # cause issue during the reset sequence from parent platform agent.
        #
        # During the shutdown sequence the following should be logged out by the platform prior to trying
        # the GO_INACTIVE command to the (already reset) instrument:
        # ion.agents.platform.platform_agent:2195 'LJ01D': instrument '8761366c8d734a4ba886606c3a47b621':
        #       current_state='RESOURCE_AGENT_STATE_UNINITIALIZED'
        #       expected_state='RESOURCE_AGENT_STATE_INACTIVE'
        #       command='RESOURCE_AGENT_EVENT_GO_INACTIVE', comp=-1, actl=decr, acceptable_state=True
        #
        self._set_receive_timeout()

        instr_keys = ['SBE37_SIM_01', 'SBE37_SIM_02']

        p_root = self._set_up_single_platform_with_some_instruments(instr_keys)
        self._launch_network(p_root)

        i_obj = self._get_instrument(instr_keys[0])
        ia_client = self._create_resource_agent_client(i_obj.instrument_device_id)
        self.assertEqual(ia_client.get_agent_state(), ResourceAgentState.COMMAND)

        # reset the instrument
        log.debug("resetting instrument %r", instr_keys[0])
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = ia_client.execute_agent(cmd, timeout=self._receive_timeout)
        log.debug("reset instrument %r returned: %s", instr_keys[0], retval)
        self.assertEqual(ia_client.get_agent_state(), ResourceAgentState.UNINITIALIZED)

        # and let the test complete with regular shutdown sequence.
