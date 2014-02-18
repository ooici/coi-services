"""
@package ion.agents.platform.test.test_mission_executive
@file    ion/agents/platform/test/test_test_mission_executive.py
@author  Bob Fratantonio
@brief   Test cases for platform mission executive. Based on 
        ion/agents/platform/test/test_platform_agent_with_rsn.py
"""

import unittest
from unittest import skip
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr

from pyon.public import log
from ion.agents.platform.rsn.simulator.logger import Logger
Logger.set_logger(log)

from ion.agents.mission_executive import MissionLoader, MissionScheduler
from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform
from pyon.util.breakpoint import breakpoint


@attr('UNIT', group='rjf')
class TestParseMission(PyonTestCase):
	"""
    Unit tests for the mission parser
    """
	# def setUp(self):
	# 	pass

	# def tearDownClass(self):
	# 	pass

	def test_load_YAML(self):
		mission = MissionLoader()
		filename =  "ion/agents/platform/test/mission_RSN_simulator1.yml"
		self.assertTrue(mission.load_mission_file(filename))

	def test_validate_schedule(self):
		pass


# @unittest.skipIf(os.getenv("OMS") is not None, "OMS environment variable is defined.")
@attr('INT', group='rjf')
class TestSimpleMission(BaseIntTestPlatform):
    """
    Test cases for the RSN OMS simulator, which is instantiated directly (ie.,
    no connection to external simulator is involved).
    """
    def load_mission(self, yaml_filename='ion/agents/platform/test/mission_RSN_simulator1.yml'):
        self.mission = MissionLoader()
        self.mission.load_mission_file(yaml_filename)

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

    @skip("Work in progress...")
    def test_simple_simulator_mission(self):

        self.load_mission()

        self._set_receive_timeout()

        instruments = []
        # mission_cmds = []
        for missionIndex in range(len(self.mission.mission_entries)): 
            instruments.append(self.mission.mission_entries[missionIndex]['instrument_id'])
            # mission_cmds.append(self.mission.mission_entries[missionIndex]['instrument_id'])


        p_root = self._set_up_single_platform_with_some_instruments(instruments)
        self._start_platform(p_root)
        # breakpoint(locals())
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self.missionSchedule = MissionScheduler(self._pa_client, self.mission.mission_entries)

        # self._run_startup_commands()

        # self._start_resource_monitoring()

        # self._wait_for_a_data_sample()

        # i_obj1 = self._get_instrument('SBE37_SIM_01')
        # #check that the instrument is in streaming mode.
        # _ia_client1 = self._create_resource_agent_client(i_obj1.instrument_device_id)
        # state1 = _ia_client1.get_agent_state()
        # self.assertEquals(state1, ResourceAgentState.STREAMING)

        # i_obj2 = self._get_instrument('SBE37_SIM_02')
        # #check that the instrument is in streaming mode.
        # _ia_client2 = self._create_resource_agent_client(i_obj2.instrument_device_id)
        # state2 = _ia_client2.get_agent_state()
        # self.assertEquals(state2, ResourceAgentState.STREAMING)

        # self._stop_resource_monitoring()

        # #check that the instrument is NOT in streaming mode.
        # state1 = _ia_client1.get_agent_state()
        # self.assertEquals(state1, ResourceAgentState.COMMAND)

        # state2 = _ia_client2.get_agent_state()
        # self.assertEquals(state2, ResourceAgentState.COMMAND)

