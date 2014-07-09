"""
@package ion.agents.platform.test.test_mission_executive
@file    ion/agents/platform/test/test_mission_executive.py
@author  Bob Fratantonio
@brief   Unit test cases for platform mission executive.
"""

# Import base test class first.
from pyon.util.unit_test import PyonTestCase

# Standard library imports.
from nose.plugins.attrib import attr

# Pyon imports.
from pyon.public import log
from pyon.util.containers import DotDict

# Ion imports.
from ion.agents.platform.rsn.simulator.logger import Logger
Logger.set_logger(log)
from ion.agents.mission_executive import MissionLoader, MissionScheduler


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
