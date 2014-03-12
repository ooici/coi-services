#!/usr/bin/env python

"""
@package ion.agents.platform.test.test_mission_exec
@file    ion/agents/platform/test/test_mission_exec.py
@author  Edward Hunter
@brief   Test cases mission exec opt-in classes.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Import pyon test class first.
from pyon.util.int_test import IonIntegrationTestCase


# Nose imports.
from nose.plugins.attrib import attr

# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_exec.py:TestMissionExec
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_exec.py:TestMissionExec.test_mission_loader
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_mission_exec.py:TestMissionExec.test_mission_exec


@attr('INT', group='sa')
class TestMissionExec(IonIntegrationTestCase):
    """
    Test cases mission exec opt-in classes.
    """

    def setUp(self):
        """
        Common test setup.
        @return:
        """
        pass


    def test_mission_loader(self):
        """
        Test mission loader class.
        @return:
        """
        pass


    def test_mission_exec(self):
        """
        Test mission executive class.
        @return:
        """
        pass
