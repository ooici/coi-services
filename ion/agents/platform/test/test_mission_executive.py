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
from pyon.event.event import EventPublisher
from pyon.public import log
from pyon.public import IonObject
from pyon.public import RT, PRED
from pyon.util.context import LocalContextMixin
from pyon.util.breakpoint import breakpoint

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


class SimulateShallowWaterProfilerEvents(object):
    """
    A fake event generator for RSN Shallow Water Profiler
    """
    def __init__(self):
        self.profiler_resource_id = 'FakeID'
        self.seconds_between_steps = 120
        self.num_steps = 10
        self.num_profiles = 2

        self.simulate_profiler_events()

    def profiler_event_state_change(self, state, sleep_duration):
        """
        Publish the state change event

        @param state            the state entered by the driver.
        @param sleep_duration   seconds to sleep for after event
        """
        # Create event publisher.
        event_data = {'state': state}
        self._event_publisher.publish_event(event_type='ResourceAgentResourceStateEvent',
                                            origin=self.profiler_resource_id,
                                            **event_data)
        gevent.sleep(sleep_duration)

    def simulate_profiler_events(self):
        """
        Simulate the Shallow Water profiler stair step mission
        """
        self._event_publisher = EventPublisher(event_type="ResourceAgentResourceStateEvent")

        # Let's simulate a profiler stair step scenario
        for x in range(self.num_profiles):
            # Going up
            for up in range(self.num_steps):
                state = 'atStep'
                self.profiler_event_state_change(state, self.seconds_between_steps)

            state = 'atCeiling'
            self.profiler_event_state_change(state, self.seconds_between_steps)

            # Going down
            for up in range(self.num_steps):
                state = 'atStep'
                self.profiler_event_state_change(state, self.seconds_between_steps)

            state = 'atFloor'
            self.profiler_event_state_change(state, self.seconds_between_steps)


@attr('UNIT')
class TestParseMission(PyonTestCase):
    """
    Unit tests for the mission parser
    """

    def test_load_YAML(self):
        mission = MissionLoader()
        filename = "ion/agents/platform/test/mission_RSN_simulator1.yml"
        self.assertTrue(mission.load_mission_file(filename))


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
            instrument_id = mission['instrument_id']
            # create only if not already created:
            if instrument_id in self._setup_instruments:
                i_obj = self._setup_instruments[instrument_id]
            else:
                i_obj = self._create_instrument(instrument_id, start_port_agent=True)
            self._assign_instrument_to_platform(i_obj, p_root)

        # Start the platform
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._instruments = {}
        # Now get instrument clients for each instrument
        for mission in self.mission.mission_entries:
            instrument_id = mission['instrument_id']
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

    def simulate_profiler_events(self):
        """
        Simulate the Shallow Water profiler stair step mission
        """
        event_publisher = EventPublisher(event_type="ResourceAgentResourceStateEvent")

        profiler_resource_id = 'FakeID'
        seconds_between_steps = 120
        num_steps = 10
        num_profiles = 2

        def profiler_event_state_change(state, sleep_duration):
            """
            Publish the state change event

            @param state            the state entered by the driver.
            @param sleep_duration   seconds to sleep for after event
            """
            # Create event publisher.
            event_data = {'state': state}
            event_publisher.publish_event(event_type='ResourceAgentResourceStateEvent',
                                          origin=profiler_resource_id,
                                          **event_data)
            gevent.sleep(sleep_duration)

        # Let's simulate a profiler stair step scenario
        for x in range(num_profiles):
            # Going up
            state = 'atStep'
            for up in range(num_steps):
                profiler_event_state_change(state, seconds_between_steps)

            state = 'atCeiling'
            profiler_event_state_change(state, seconds_between_steps)

            # Going down
            state = 'atStep'
            for down in range(num_steps):
                profiler_event_state_change(state, seconds_between_steps)

            state = 'atFloor'
            profiler_event_state_change(state, seconds_between_steps)

    def load_mission(self, yaml_filename='ion/agents/platform/test/mission_RSN_simulator1.yml'):
        """
        Load and parse the mission file
        """
        self.mission = MissionLoader()
        self.mission.load_mission_file(yaml_filename)

    # @skip("Work in progress...")
    def test_simple_simulator_mission(self):
        """
        Test the RSN OMS platform simulator with the SBE37_SIM instruments
        """
        filename = "ion/agents/platform/test/mission_RSN_simulator1.yml"
        self.load_mission(yaml_filename=filename)

        self.setup_platform_simulator_and_instruments()

        # Start Mission Scheduer
        self.missionSchedule = MissionScheduler(self._pa_client, self._instruments, self.mission.mission_entries)

    # @skip("Work in progress...")
    def test_shallow_profiler_mission(self):
        """
        Test the Shallow Water Profiler mission
        """
        filename = 'ion/agents/platform/test/mission_ShallowProfiler.yml'
        self.load_mission(yaml_filename=filename)

        # Setup the platform and instruments
        self.setup_platform_simulator_and_instruments()

        # Start profiler event simulator and mission scheduler
        self.threads = []
        self.threads.append(gevent.spawn(
            MissionScheduler, self._pa_client, self._instruments, self.mission.mission_entries))
        # self.threads.append(gevent.spawn_later(30, SimulateShallowWaterProfilerEvents()))
        self.threads.append(gevent.spawn_later(60, self.simulate_profiler_events()))

        gevent.joinall(self.threads)

        # Start Mission Scheduer
        # self.missionSchedule = MissionScheduler(self._pa_client, self._instruments, self.mission.mission_entries)
