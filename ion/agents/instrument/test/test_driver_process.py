#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_driver_process
@file ion/agents.instrument/test_driver_process.py
@author Edward Hunter
@brief Test cases for DriverProcess.
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import time
import unittest

from nose.plugins.attrib import attr
from pyon.util.unit_test import PyonTestCase

from pyon.public import log

from ion.agents.instrument.driver_process import DriverProcess, DriverProcessType

# Make tests verbose and provide stdout
# bin/nosetests -s -v ion/agents.instrument/test/test_driver_launcher.py

@attr('UNIT', group='mi')
class TestPythonClassDriverProcess(PyonTestCase):
    """
    Unit tests for Driver Process using python classes
    """
    def setUp(self):
        """
        Setup test cases.
        """
        self._driver_config = { 'host': 'localhost',
            'cmd_port': 5556,
            'evt_port': 5557,

            'dvr_mod': 'mi.instrument.seabird.sbe37smb.example.driver',
            'dvr_cls': 'exampleInstrumentDriver',

            'process_type': DriverProcessType.PYTHON_MODULE
        }


        # Add cleanup handler functions.
        # self.addCleanup()

    def event_received(self):
        log.debug("Event received")

    def test_driver_process(self):
        """
        Test driver process launch
        """
        driver_process = DriverProcess.get_process(self._driver_config, True)
        self.assertTrue(driver_process)

        driver_process.launch()
        self.assertTrue(driver_process.getpid())
        log.info("Driver memory usage after init: %d", driver_process.memory_usage())

        self.assertGreater(driver_process._command_port, 0)
        self.assertGreater(driver_process._event_port, 0)

        driver_client = driver_process.get_client()
        self.assertTrue(driver_client)

        driver_client.start_messaging(self.event_received())

        packet_factories = driver_process.get_packet_factories()
        self.assertTrue(packet_factories)

        self.assertGreater(driver_process.memory_usage(), 0)
        log.info("Driver memory usage before stop: %d", driver_process.memory_usage())

        driver_process.stop()
        self.assertFalse(driver_process.getpid())





@attr('UNIT', group='mi')
class TestEggDriverProcess(PyonTestCase):
    """
    Unit tests for Driver Process using eggs
    """
    
    def setUp(self):
        """
        Setup test cases.
        """
        pass

