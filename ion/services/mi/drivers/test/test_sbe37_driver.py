#!/usr/bin/env python

"""
@package ion.services.mi.test.test_sbe37_driver
@file ion/services/mi/test_sbe37_driver.py
@author Edward Hunter
@brief Test cases for SBE37Driver
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Ensure the test class is monkey patched for gevent
from gevent import monkey; monkey.patch_all()

# Standard lib imports
import time
import unittest
import logging
from subprocess import Popen
import os
import signal

# 3rd party imports
from nose.plugins.attrib import attr

# Pyon and ION imports
from pyon.util.unit_test import PyonTestCase
from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess
from ion.services.mi.drivers.sbe37_driver import SBE37Driver
from ion.services.mi.logger_process import EthernetDeviceLogger

# MI logger
import ion.services.mi.mi_logger
mi_logger = logging.getLogger('mi_logger')

# Make tests verbose and provide stdout
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_process
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_config
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_connect
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_get_set
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_poll
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_autosample

# Driver and port agent configuration
DVR_SVR_ADDR = 'localhost' # Addr of the driver process
DVR_CMD_PORT = 5556 # Command port of the driver process
DVR_EVT_PORT = 5557 # Event port of the driver process
DVR_MOD = 'ion.services.mi.drivers.sbe37_driver' # Driver module
DVR_CLS = 'SBE37Driver' # Driver class
DEV_ADDR = '137.110.112.119'
DEV_PORT = 4001
PAGENT_ADDR = 'localhost'
PAGENT_PORT = 8888
COMMS_CONFIG = {
    'addr': PAGENT_ADDR,
    'port': PAGENT_PORT
}

@attr('HARDWARE', group='mi')
class TestSBE37Driver(PyonTestCase):    
    """
    Integration tests for the sbe37 driver. This class tests and shows
    use patterns for the sbe37 driver as a zmq driver process.
    """    
    
    def setUp(self):
        """
        Setup test cases.
        """

        # Clear driver event list.
        self._events = []

        # The port agent object. Used to start and stop the port agent.
        self._pagent = None
        
        # The driver process popen object.
        self._dvr_proc = None
        
        # The driver client.
        self._dvr_client = None

        # Create and start the port agent.
        mi_logger.info('start')
        self._start_pagent()
        self.addCleanup(self._stop_pagent)    

        # Create and start the driver.
        self._start_driver()
        self.addCleanup(self._stop_driver)        
        
    def _start_pagent(self):
        """
        """
        self._pagent = EthernetDeviceLogger(DEV_ADDR, DEV_PORT, PAGENT_PORT)
        mi_logger.info('Created port agent object for %s %d %d', DEV_ADDR,
                       DEV_PORT, PAGENT_PORT)
        self._stop_pagent()
        pid = None
        self._pagent.start()
        pid = self._pagent.get_pid()
        while not pid:
            time.sleep(.1)
            pid = self._pagent.get_pid()
        mi_logger.info('Started port agent pid %d', pid)
        
    def _stop_pagent(self):
        """
        """
        if self._pagent:
            pid = self._pagent.get_pid()
            if pid:
                mi_logger.info('Stopping pagent pid %s', pid)
                self._pagent.stop()
            else:
                mi_logger.info('No port agent running.')
            
    def _start_driver(self):
        """
        """
        # Launch driver process.
        self._dvr_proc = ZmqDriverProcess.launch_process(DVR_CMD_PORT,
            DVR_EVT_PORT, DVR_MOD, DVR_CLS)
        mi_logger.info('Started driver process for %d %d %s %s', DVR_CMD_PORT,
            DVR_EVT_PORT, DVR_MOD, DVR_CLS)
        mi_logger.info('Driver process pid %d', self._dvr_proc.pid)
            
        # Create driver client.            
        self._dvr_client = ZmqDriverClient(DVR_SVR_ADDR, DVR_CMD_PORT,
            DVR_EVT_PORT)
        mi_logger.info('Created driver client for %d %d %s %s', DVR_CMD_PORT,
            DVR_EVT_PORT, DVR_MOD, DVR_CLS)
        
        # Start client messaging.
        self._dvr_client.start_messaging(self.evt_recd)
        mi_logger.info('Driver messaging started.')
        time.sleep(.5)
            
    def _stop_driver(self):
        """
        Method to shut down the driver process. Attempt normal shutdown,
        and kill the process if unsuccessful.
        """
        
        if self._dvr_proc:
            mi_logger.info('Stopping driver process pid %d', self._dvr_proc.pid)
            if self._dvr_client:
                self._dvr_client.done()
                self._dvr_proc.wait()
                self._dvr_client = None

            else:
                try:
                    mi_logger.info('Killing driver process.')
                    self._dvr_proc.kill()
                except OSError:
                    pass
            self._dvr_proc = None

    def evt_recd(self, evt):
        """
        Simple callback to catch events from the driver for verification.
        """
        self._events.append(evt)
    
    def test_process(self):
        """
        Test for correct launch of driver process and communications, including
        asynchronous driver events.
        """

        # Add test to verify process exists.        
        
        # Send a test message to the process interface, confirm result.
        msg = 'I am a ZMQ message going to the process.'
        reply = self._dvr_client.cmd_dvr('process_echo', msg)
        self.assertEqual(reply,'process_echo: '+msg)
        
        
        # Send a test message to the driver interface, confirm result.
        msg = 'I am a ZMQ message going to the driver.'
        reply = self._dvr_client.cmd_dvr('driver_echo', msg)
        self.assertEqual(reply, 'driver_echo: '+msg)
        
        # Test the event thread publishes and client side picks up events.
        events = [
            'I am important event #1!',
            'And I am important event #2!'
            ]
        reply = self._dvr_client.cmd_dvr('test_events', events=events)
        time.sleep(2)
        
        # Confirm the events received are as expected.
        self.assertEqual(self._events, events)
        
    def test_config(self):
        """
        Test to configure the driver process for device comms and transition
        to disconnected state.
        """

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('configure', COMMS_CONFIG)

        # Initialize the driver and transition to unconfigured.
        reply = self._dvr_client.cmd_dvr('initialize')
        
            
    
    def test_connect(self):
        """
        """
        
        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('configure', COMMS_CONFIG)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('connect')

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('discover')

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('disconnect')

        # Initialize the driver and transition to unconfigured.
        reply = self._dvr_client.cmd_dvr('initialize')
    


    def test_poll(self):
        """
        Test sample polling commands and events.
        """

        reply = self._dvr_client.cmd_dvr('configure', COMMS_CONFIG)

        reply = self._dvr_client.cmd_dvr('connect')
                
        reply = self._dvr_client.cmd_dvr('discover')

        reply = self._dvr_client.cmd_dvr('execute_acquire_sample')
        
        reply = self._dvr_client.cmd_dvr('execute_acquire_sample')

        reply = self._dvr_client.cmd_dvr('execute_acquire_sample')

        #print 'EVENTS RECEIVED:'
        #print str(self.events)

        reply = self._dvr_client.cmd_dvr('disconnect')
        
        # Deconfigure the driver.
        reply = self._dvr_client.cmd_dvr('initialize')
        
