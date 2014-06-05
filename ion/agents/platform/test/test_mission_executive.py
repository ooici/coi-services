"""
@package ion.agents.platform.test.test_mission_executive
@file    ion/agents/platform/test/test_test_mission_executive.py
@author  Bob Fratantonio
@brief   Test cases for platform mission executive. Based on
        ion/agents/platform/test/test_platform_agent_with_rsn.py
"""

# Import base test class first.
from pyon.util.unit_test import PyonTestCase
from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform

# Standard library imports.
import os
import unittest
from unittest import skip
from nose.plugins.attrib import attr
import gevent

# Pyon imports.
from pyon.agent.agent import ResourceAgentClient
from pyon.agent.common import BaseEnum
from pyon.event.event import EventPublisher
from pyon.public import log
from pyon.public import IonObject
from pyon.public import RT, PRED
from pyon.util.config import Config
from pyon.util.context import LocalContextMixin
from pyon.util.containers import DotDict


# Ion imports.
from ion.agents.platform.rsn.simulator.logger import Logger
Logger.set_logger(log)
from ion.agents.mission_executive import MissionLoader, MissionScheduler

# Interface imports.
from interface.objects import AttachmentType

# Mock imports.
# from mock import Mock, patch


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id = ''
    process_type = ''


# to fake a resource agent client
class FakeAgent(object):

    def __init__(self):
        self.cmds = {}

    def get_agent(self, cmds):
        return dict([(c, self.cmds.get(c, None)) for c in cmds])

    def set_agent(self, key, val):
        self.cmds[key] = val

    def get_capabilities(self):
        return [DotDict({"name": k}) for k in self.cmds.keys()]

    def get_agent_state(self):
        return "FAKE"


class MissionEvents(BaseEnum):
    """
    Acceptable fake mission events.
    """
    #Shallow Profiler Events
    PROFILER_AT_CEILING = 'atCeiling'
    PROFILER_AT_FLOOR = 'atFloor'
    PROFILER_AT_STEP = 'atStep'
    PROFILER_AT_DEPTH = 'atDepth'
    PROFILER_GO_TO_COMPLETE = 'gotocomplete'
    PROFILER_IDLE_TIMEOUT = 'idletimeout'
    PROFILER_SYSTEM_ERROR = 'systemerror'
    PROFILER_MISSION_COMPLETE = 'missioncomplete'


@attr('UNIT')
class TestParseMission(PyonTestCase):
    """
    Unit tests for the mission parser
    """

    def test_load_mission_file(self):
        p_agent = FakeAgent()
        mission = MissionLoader(p_agent)
        filename = "ion/agents/platform/test/mission_RSN_simulator1.yml"
        self.assertTrue(mission.load_mission_file(filename))

    def test_load_mission(self):
        p_agent = FakeAgent()
        mission_id = 0
        mission = MissionLoader(p_agent)
        filename = "ion/agents/platform/test/mission_RSN_simulator1.yml"

        with open(filename, 'r') as f:
            mission_string = f.read()

        self.assertTrue(mission.load_mission(mission_id, mission_string))


# @unittest.skipIf(os.getenv("OMS") is not None, "OMS environment variable is defined.")
@attr('INT')
class TestSimpleMission(BaseIntTestPlatform, PyonTestCase):
    """
    Test cases for the RSN OMS simulator, which is instantiated directly (ie.,
    no connection to external simulator is involved).
    """

    def _run_shutdown_commands(self, recursion=True):
        try:
            self._go_inactive(recursion)
            self._reset(recursion)
        finally:  # attempt shutdown anyway
            self._shutdown(True)  # NOTE: shutdown always with recursion=True

    def setup_platform_simulator_and_instruments(self):
        self._set_receive_timeout()

        # Create a platform in the test environment
        p_root = self._create_single_platform()
        # Create instruments and assign to platform
        for mission in self.mission.mission_entries:
            for instrument_id in mission['instrument_id']:
                # create only if not already created:
                if instrument_id not in self._setup_instruments:
                    i_obj = self._create_instrument(instrument_id, start_port_agent=True)
                    self._assign_instrument_to_platform(i_obj, p_root)

        # Start the platform
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        # self.addCleanup(self._run_shutdown_commands)

        self._instruments = {}

        self._instruments.update({self.PLATFORM_ID: self._pa_client})
        # Now get instrument clients for each instrument
        for mission in self.mission.mission_entries:
            for instrument_id in mission['instrument_id']:
                instrument_device_id = self._setup_instruments[instrument_id]['instrument_device_id']

                # Start a resource agent client to talk with each instrument agent.
                ia_client = ResourceAgentClient(instrument_device_id, process=FakeProcess())
                # make a dictionary storing the instrument ids and client objects
                self._instruments.update({instrument_id: ia_client})


    def get_mission_attachment(self, filename):
        """
        Treat the mission file as if it were a platform attachment
        """
        # read the file
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

        self.load_mission(yaml_filename=temp_file)

        if os.path.isfile(temp_file):
            os.remove(temp_file)

        return p_root

    def simulate_profiler_events(self, profile_type):
        """
        Simulate the Shallow Water profiler stair step mission
        """
        event_publisher = EventPublisher(event_type="OMSDeviceStatusEvent")

        num_profiles = 2

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

            seconds_between_steps = 60
            num_steps = 2

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
                profiler_event_state_change('atCeiling', seconds_between_steps)
                # Start to descend
                profiler_event_state_change('StartingDescent', seconds_between_steps)
                # Arrive at floor
                profiler_event_state_change('atFloor', seconds_between_steps)

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

    def load_mission(self, yaml_filename='ion/agents/platform/test/mission_RSN_simulator1.yml'):
        """
        Load and parse the mission file
        """
        self.mission = MissionLoader('')
        self.mission.load_mission_file(yaml_filename)

    @skip("Deprecated... Use test_mission_manager instead")
    def test_simple_simulator_mission(self):
        """
        Test the RSN OMS platform simulator with the SBE37_SIM instruments
        """
        filename = "ion/agents/platform/test/mission_RSN_simulator1.yml"

        self.load_mission(yaml_filename=filename)
        log.debug('mission_entries=%s', self.mission.mission_entries)

        self.setup_platform_simulator_and_instruments()

        # Start Mission Scheduer
        self.missionSchedule = MissionScheduler(self._pa_client, self._instruments, self.mission.mission_entries)
        self.missionSchedule.run_mission()

    @skip("Work in progress...")
    def test_shallow_profiler_mission(self):
        """
        Test the Shallow Water Profiler mission
        """
        filename = 'ion/agents/platform/test/mission_ShallowProfiler_simulated.yml'
        self.load_mission(yaml_filename=filename)

        # Setup the platform and instruments
        self.setup_platform_simulator_and_instruments()

        # Start Mission Scheduer
        self.missionSchedule = MissionScheduler(self._pa_client, self._instruments, self.mission.mission_entries)

        # Start profiler event simulator and mission scheduler
        self.threads = []
        self.threads.append(gevent.spawn(self.missionSchedule.run_mission))
        # self.threads.append(gevent.spawn_later(30, SimulateShallowWaterProfilerEvents()))
        self.threads.append(gevent.spawn_later(120, self.simulate_profiler_events, 'stair_step'))

        gevent.joinall(self.threads)

        # Start Mission Scheduer
        # self.missionSchedule = MissionScheduler(self._pa_client, self._instruments, self.mission.mission_entries)
        # self.missionSchedule.run_mission()
