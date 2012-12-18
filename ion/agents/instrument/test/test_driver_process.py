#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_driver_process
@file ion/agents.instrument/test_driver_process.py
@author Edward Hunter
@brief Test cases for DriverProcess.
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import os
import time
import unittest
from pyon.public import CFG
from nose.plugins.attrib import attr
from pyon.util.unit_test import PyonTestCase

from pyon.public import log

from ion.agents.instrument.driver_process import DriverProcess, DriverProcessType, ZMQEggDriverProcess
from ion.agents.instrument.exceptions import DriverLaunchException

# Make tests verbose and provide stdout
# bin/nosetests -s -v ion/agents.instrument/test/test_driver_launcher.py

@attr('UNIT', group='mi')
class TestInstrumentDriverProcess(PyonTestCase):
    """
    Unit tests for Driver Process using python classes
    """
    def setUp(self):
        """
        Setup test cases.
        """
        self._class_driver_config = {
            'host': 'localhost',
            'cmd_port': 5556,
            'evt_port': 5557,

            'dvr_mod': 'mi.instrument.seabird.sbe37smb.example.driver',
            'dvr_cls': 'InstrumentDriver',

            'process_type': [DriverProcessType.PYTHON_MODULE]
        }

        self._egg_driver_config = {
            'host': 'localhost',
            'cmd_port': 5556,
            'evt_port': 5557,

            'dvr_egg': 'seabird_sbe37smb_ooicore-0.0.1-py2.7.egg',

            'process_type': [DriverProcessType.EGG]
        }

        self._events = []


        # Add cleanup handler functions.
        # self.addCleanup()

    def event_received(self, event):
        log.debug("Event received")
        self._events.append(event)

    def assert_driver_process_launch_success(self, driver_config):
        """
        Verify that we can launch a driver using a driver config.
        @param driver_config: driver configuration dictionary
        """
        driver_process = DriverProcess.get_process(driver_config, True)
        self.assertTrue(driver_process)

        driver_process.launch()
        self.assertTrue(driver_process.getpid())

        # command not portable
        #log.info("Driver memory usage after init: %d", driver_process.memory_usage())

        self.assertGreater(driver_process._command_port, 0)
        self.assertGreater(driver_process._event_port, 0)

        driver_client = driver_process.get_client()
        self.assertTrue(driver_client)

        driver_client.start_messaging(self.event_received)

        # command not portable
        # self.assertGreater(driver_process.memory_usage(), 0)
        # log.info("Driver memory usage before stop: %d", driver_process.memory_usage())

        driver_process.stop()
        self.assertFalse(driver_process.getpid())

    def test_driver_process_by_class(self):
        """
        Test the driver launching process for a class and module
        """
        self.assert_driver_process_launch_success(self._class_driver_config)

    def test_driver_process_by_egg(self):
        """
        Test the driver launching process for a class and module
        """
        try:
            os.unlink("/tmp/seabird_sbe37smb_ooicore-0.0.1-py2.7.egg")
        except:
            """
            # ignore this exception.
            """

        # remove egg from cache and run.  Verifies download
        self.assert_driver_process_launch_success(self._egg_driver_config)

        # Verify egg is in cache then run again
        self.assert_driver_process_launch_success(self._egg_driver_config)

    def test_01_check_cache_for_egg(self):
        """
        Test _check_cache_for_egg  checks the cache for the egg,
        returns path if present locally, or None if not.
        """
        # Cleanup on isle one!
        try:
            os.unlink("/tmp/seabird_sbe37smb_ooicore-0.0.1-py2.7.egg")
        except:
            """
            # ignore this exception.
            """

        launcher = ZMQEggDriverProcess("DUMMY_VAL")
        self.assertEqual(launcher._check_cache_for_egg("NOT_FOUND_EGG"), None)
        self.assertEqual(launcher._check_cache_for_egg("seabird_sbe37smb_ooicore-0.0.1-py2.7.egg"), None)


    def test_02_get_remote_egg(self):
        """
        Test _get_remote_egg should return path for cached egg if present,
        path for cached egg if not present locally, but in repo, or exception if not present locally or in repo.
        """

        launcher = ZMQEggDriverProcess("DUMMY_VAL")
        got_exception = False
        try:
            self.assertEqual(launcher._get_remote_egg("NOT_FOUND_EGG"), None)
        except DriverLaunchException:
            got_exception = True
        self.assertTrue(got_exception)

        self.assertEqual(launcher._get_remote_egg("seabird_sbe37smb_ooicore-0.0.1-py2.7.egg"), "/tmp/seabird_sbe37smb_ooicore-0.0.1-py2.7.egg")


    def test_03_check_cache_for_egg(self):
        """
        Test _check_cache_for_egg  checks the cache for the egg,
        returns path if present locally, or None if not.
        """
        # Cleanup on isle one!

        launcher = ZMQEggDriverProcess("DUMMY_VAL")
        self.assertEqual(launcher._check_cache_for_egg("NOT_FOUND_EGG"), None)
        self.assertEqual(launcher._check_cache_for_egg("seabird_sbe37smb_ooicore-0.0.1-py2.7.egg"), "/tmp/seabird_sbe37smb_ooicore-0.0.1-py2.7.egg")

    def test_04_get_egg(self):
        """
        Test _get_egg should return a path to a local egg for existing
        eggs, and exception for non-existing in the repo.
        """

        launcher = ZMQEggDriverProcess("DUMMY_VAL")

        got_exception = False
        try:
            self.assertEqual(launcher._get_egg("NOT_FOUND_EGG"), None)
        except DriverLaunchException:
            got_exception = True
        self.assertTrue(got_exception)
        self.assertEqual(launcher._get_egg("seabird_sbe37smb_ooicore-0.0.1-py2.7.egg"), "/tmp/seabird_sbe37smb_ooicore-0.0.1-py2.7.egg")
