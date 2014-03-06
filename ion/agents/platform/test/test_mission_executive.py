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
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr

from pyon.public import log
from ion.agents.platform.rsn.simulator.logger import Logger
Logger.set_logger(log)

from ion.agents.mission_executive import MissionLoader, MissionScheduler
from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform

from pyon.agent.agent import ResourceAgentState

from interface.objects import AgentCommand
from interface.objects import AttachmentType
from pyon.public import IonObject
from pyon.public import RT, PRED

from pyon.util.breakpoint import breakpoint
import os

@attr('UNIT')
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
@attr('INT')
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

        # Dump mission file contents to IonObject
        filename =  "ion/agents/platform/test/mission_RSN_simulator1.yml"
        
        with open(filename, 'r') as rfile:
            content = rfile.read()
            
        # make an attachment
        attachment = IonObject(RT.Attachment,
                               name="Example mission",
                               description="Mission File",
                               content=content,
                               content_type="text/yml",
                               keywords=["mission"],
                               attachment_type=AttachmentType.ASCII)

        # Create a platform in the test environment
        p_root = self._create_single_platform()

        self.RR2.create_attachment(p_root['platform_device_id'], attachment)

        attachments, _ = self.RR.find_objects(p_root['platform_device_id'], PRED.hasAttachment, RT.Attachment, True)
        self.assertEqual(len(attachments), 1)

        a = self.RR.read_attachment(attachments[0], include_content=True)
        
        # Write contents of attached mission file to temp yaml file
        temp_file = 'temp_mission.yml'
        with open(temp_file, 'w') as wfile:
            wfile.write(a.content)

        self.load_mission(yaml_filename = temp_file)

        self._set_receive_timeout()

        instruments = []

        for missionIndex in range(len(self.mission.mission_entries)): 
            instruments.append(self.mission.mission_entries[missionIndex]['instrument_id'])

        # p_root = self._set_up_single_platform_with_some_instruments(instruments)
        
        # for instr_key in instruments:
        #     self.assertIn(instr_key, instruments_dict)
        # create and assign instruments:
        for instr_key in instruments:
            # create only if not already created:
            if instr_key in self._setup_instruments:
                i_obj = self._setup_instruments[instr_key]
            else:
                i_obj = self._create_instrument(instr_key, start_port_agent=True)
            self._assign_instrument_to_platform(i_obj, p_root)

        self._start_platform(p_root)
        
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self.missionSchedule = MissionScheduler(self._pa_client, self._setup_instruments, self.mission.mission_entries)
        
        if os.path.isfile(temp_file):
            os.remove(temp_file)

# class TestPlatformMission(IonIntegrationTestCase):
#     """
#     Test cases for the RSN shallow profiler.
#     """


