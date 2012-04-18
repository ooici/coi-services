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
import gevent

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
from ion.services.mi.drivers.sbe37_driver import SBE37ProtocolState
from ion.services.mi.drivers.sbe37_driver import SBE37Parameter
from ion.services.mi.instrument_driver import DriverAsyncEvent
from ion.services.mi.instrument_driver import DriverConnectionState
from ion.services.mi.logger_process import EthernetDeviceLogger
from ion.services.mi.exceptions import InstrumentException
from ion.services.mi.exceptions import NotImplementedError
from ion.services.mi.exceptions import TimeoutError
from ion.services.mi.exceptions import ProtocolError
from ion.services.mi.exceptions import ParameterError
from ion.services.mi.exceptions import SampleError
from ion.services.mi.exceptions import StateError
from ion.services.mi.exceptions import UnknownCommandError


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
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_test
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_errors
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_discover_autosample


# Driver and port agent configuration
DVR_SVR_ADDR = 'localhost'
DVR_CMD_PORT = 5556
DVR_EVT_PORT = 5557
DVR_MOD = 'ion.services.mi.drivers.sbe37_driver'
DVR_CLS = 'SBE37Driver'
#DEV_ADDR = '67.58.49.220' 
DEV_ADDR = '137.110.112.119' 
DEV_PORT = 4001
PAGENT_ADDR = 'localhost'
PAGENT_PORT = 8888
WORK_DIR = '/tmp/'
DELIM = ['<<','>>']
SNIFFER_PORT = None
COMMS_CONFIG = {
    'addr': PAGENT_ADDR,
    'port': PAGENT_PORT
}

PARAMS = {
    SBE37Parameter.OUTPUTSAL : bool,
    SBE37Parameter.OUTPUTSV : bool,
    SBE37Parameter.NAVG : int,
    SBE37Parameter.SAMPLENUM : int,
    SBE37Parameter.INTERVAL : int,
    SBE37Parameter.STORETIME : bool,
    SBE37Parameter.TXREALTIME : bool,
    SBE37Parameter.SYNCMODE : bool,
    SBE37Parameter.SYNCWAIT : int,
    SBE37Parameter.TCALDATE : tuple,
    SBE37Parameter.TA0 : float,
    SBE37Parameter.TA1 : float,
    SBE37Parameter.TA2 : float,
    SBE37Parameter.TA3 : float,
    SBE37Parameter.CCALDATE : tuple,
    SBE37Parameter.CG : float,
    SBE37Parameter.CH : float,
    SBE37Parameter.CI : float,
    SBE37Parameter.CJ : float,
    SBE37Parameter.WBOTC : float,
    SBE37Parameter.CTCOR : float,
    SBE37Parameter.CPCOR : float,
    SBE37Parameter.PCALDATE : tuple,
    SBE37Parameter.PA0 : float,
    SBE37Parameter.PA1 : float,
    SBE37Parameter.PA2 : float,
    SBE37Parameter.PTCA0 : float,
    SBE37Parameter.PTCA1 : float,
    SBE37Parameter.PTCA2 : float,
    SBE37Parameter.PTCB0 : float,
    SBE37Parameter.PTCB1 : float,
    SBE37Parameter.PTCB2 : float,
    SBE37Parameter.POFFSET : float,
    SBE37Parameter.RCALDATE : tuple,
    SBE37Parameter.RTCA0 : float,
    SBE37Parameter.RTCA1 : float,
    SBE37Parameter.RTCA2 : float
    
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
        Construct and start the port agent.
        """
        
        # Create port agent object.
        this_pid = os.getpid()
        self._pagent = EthernetDeviceLogger(DEV_ADDR, DEV_PORT, PAGENT_PORT,
                        WORK_DIR, DELIM, SNIFFER_PORT, this_pid)
        mi_logger.info('Created port agent object for %s %d %d', DEV_ADDR,
                       DEV_PORT, PAGENT_PORT)
        
        # Stop the port agent if it is already running.
        # The port agent creates a pid file based on the config used to
        # construct it.
        self._stop_pagent()
        pid = None

        # Start the port agent.
        # Confirm it is started by getting pidfile.
        self._pagent.start()
        pid = self._pagent.get_pid()
        while not pid:
            time.sleep(.1)
            pid = self._pagent.get_pid()
        mi_logger.info('Started port agent pid %d', pid)
        
    def _stop_pagent(self):
        """
        Stop the port agent.
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
        Start the driver process.
        """
        
        # Launch driver process based on test config.
        this_pid = os.getpid()
        self._dvr_proc = ZmqDriverProcess.launch_process(DVR_CMD_PORT,
            DVR_EVT_PORT, DVR_MOD, DVR_CLS, this_pid)
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
    
    def assertSampleDict(self, val):
        """
        Verify the value is a sample dictionary for the sbe37.
        """
        #{'p': [-6.945], 'c': [0.08707], 't': [20.002], 'time': [1333752198.450622]}        
        self.assertTrue(isinstance(val, dict))
        self.assertTrue(val.has_key('c'))
        self.assertTrue(val.has_key('t'))
        self.assertTrue(val.has_key('p'))
        self.assertTrue(val.has_key('time'))
        c = val['c'][0]
        t = val['t'][0]
        p = val['p'][0]
        time = val['time'][0]
    
        self.assertTrue(isinstance(c, float))
        self.assertTrue(isinstance(t, float))
        self.assertTrue(isinstance(p, float))
        self.assertTrue(isinstance(time, float))
    
    def assertParamDict(self, pd, all_params=False):
        """
        Verify all device parameters exist and are correct type.
        """
        if all_params:
            self.assertEqual(set(pd.keys()), set(PARAMS.keys()))
            #print str(pd)
            #print str(PARAMS)
            for (key, type_val) in PARAMS.iteritems():
                #print key
                self.assertTrue(isinstance(pd[key], type_val))
        else:
            for (key, val) in pd.iteritems():
                self.assertTrue(PARAMS.has_key(key))
                self.assertTrue(isinstance(val, PARAMS[key]))
    
    def assertParamVals(self, params, correct_params):
        """
        Verify parameters take the correct values.
        """
        self.assertEqual(set(params.keys()), set(correct_params.keys()))
        for (key, val) in params.iteritems():
            correct_val = correct_params[key]
            if isinstance(val, float):
                # Verify to 5% of the larger value.
                max_val = max(abs(val), abs(correct_val))
                self.assertAlmostEqual(val, correct_val, delta=max_val*.01)

            else:
                # int, bool, str, or tuple of same
                self.assertEqual(val, correct_val)
    
    def test_process(self):
        """
        Test for correct launch of driver process and communications, including
        asynchronous driver events.
        """

        # Verify processes exist.
        self.assertNotEqual(self._dvr_proc, None)
        drv_pid = self._dvr_proc.pid
        self.assertTrue(isinstance(drv_pid, int))
        
        self.assertNotEqual(self._pagent, None)
        pagent_pid = self._pagent.get_pid()
        self.assertTrue(isinstance(pagent_pid, int))
        
        # Send a test message to the process interface, confirm result.
        msg = 'I am a ZMQ message going to the process.'
        reply = self._dvr_client.cmd_dvr('process_echo', msg)
        self.assertEqual(reply,'process_echo: '+msg)
        
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)
        
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
        time.sleep(1)
        
        # Confirm the events received are as expected.
        self.assertEqual(self._events, events)

        # Test the exception mechanism.
        with self.assertRaises(InstrumentException):
            exception_str = 'Oh no, something bad happened!'
            reply = self._dvr_client.cmd_dvr('test_exceptions', exception_str)
        
        # Verify we received a driver error event.
        time.sleep(1)
        error_events = [evt for evt in self._events if isinstance(evt, dict) and evt['type']==DriverAsyncEvent.ERROR]
        self.assertTrue(len(error_events) == 1)
        
    def test_config(self):
        """
        Test to configure the driver process for device comms and transition
        to disconnected state.
        """

        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('configure', COMMS_CONFIG)

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Initialize the driver and transition to unconfigured.
        reply = self._dvr_client.cmd_dvr('initialize')

        # Test the driver returned state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)
        
    def test_connect(self):
        """
        Test configuring and connecting to the device through the port
        agent. Discover device state.
        """
        
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('configure', COMMS_CONFIG)

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('connect')

        # Test the driver is in unknown state.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('discover')

        # Test the driver is in command mode.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('disconnect')

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Initialize the driver and transition to unconfigured.
        reply = self._dvr_client.cmd_dvr('initialize')
    
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)
        
    def test_get_set(self):
        """
        Test device parameter access.
        """

        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        reply = self._dvr_client.cmd_dvr('configure', COMMS_CONFIG)

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        reply = self._dvr_client.cmd_dvr('connect')
                
        # Test the driver is in unknown state.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)
                
        reply = self._dvr_client.cmd_dvr('discover')

        # Test the driver is in command mode.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        # Get all device parameters. Confirm all expected keys are retrived
        # and have correct type.
        reply = self._dvr_client.cmd_dvr('get', SBE37Parameter.ALL)
        self.assertParamDict(reply, True)

        # Remember original configuration.
        orig_config = reply
        
        # Grab a subset of parameters.
        params = [
            SBE37Parameter.TA0,
            SBE37Parameter.INTERVAL,
            SBE37Parameter.STORETIME,
            SBE37Parameter.TCALDATE
            ]
        reply = self._dvr_client.cmd_dvr('get', params)
        self.assertParamDict(reply)        

        # Remember the original subset.
        orig_params = reply
        
        # Construct new parameters to set.
        old_date = orig_params[SBE37Parameter.TCALDATE]
        new_params = {
            SBE37Parameter.TA0 : orig_params[SBE37Parameter.TA0] * 1.2,
            SBE37Parameter.INTERVAL : orig_params[SBE37Parameter.INTERVAL] + 1,
            SBE37Parameter.STORETIME : not orig_params[SBE37Parameter.STORETIME],
            SBE37Parameter.TCALDATE : (old_date[0], old_date[1], old_date[2] + 1)
        }

        # Set parameters and verify.
        reply = self._dvr_client.cmd_dvr('set', new_params)
        reply = self._dvr_client.cmd_dvr('get', params)
        self.assertParamVals(reply, new_params)
        
        # Restore original parameters and verify.
        reply = self._dvr_client.cmd_dvr('set', orig_params)
        reply = self._dvr_client.cmd_dvr('get', params)
        self.assertParamVals(reply, orig_params)

        # Retrieve the configuration and ensure it matches the original.
        # Remove samplenum as it is switched by autosample and storetime.
        reply = self._dvr_client.cmd_dvr('get', SBE37Parameter.ALL)
        reply.pop('SAMPLENUM')
        orig_config.pop('SAMPLENUM')
        self.assertParamVals(reply, orig_config)

        # Disconnect from the port agent.
        reply = self._dvr_client.cmd_dvr('disconnect')
        
        # Test the driver is disconnected.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)
        
        # Deconfigure the driver.
        reply = self._dvr_client.cmd_dvr('initialize')
        
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)        
    
    def test_poll(self):
        """
        Test sample polling commands and events.
        """

        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        reply = self._dvr_client.cmd_dvr('configure', COMMS_CONFIG)

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        reply = self._dvr_client.cmd_dvr('connect')
                
        # Test the driver is in unknown state.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)
                
        reply = self._dvr_client.cmd_dvr('discover')

        # Test the driver is in command mode.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        # Poll for a sample and confirm result.
        reply = self._dvr_client.cmd_dvr('execute_acquire_sample')
        self.assertSampleDict(reply)
        
        # Poll for a sample and confirm result.
        reply = self._dvr_client.cmd_dvr('execute_acquire_sample')
        self.assertSampleDict(reply)

        # Poll for a sample and confirm result.
        reply = self._dvr_client.cmd_dvr('execute_acquire_sample')
        self.assertSampleDict(reply)
        
        # Confirm that 3 samples arrived as published events.
        time.sleep(1)
        sample_events = [evt for evt in self._events if evt['type']==DriverAsyncEvent.SAMPLE]
        self.assertEqual(len(sample_events), 3)

        # Disconnect from the port agent.
        reply = self._dvr_client.cmd_dvr('disconnect')
        
        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)
        
        # Deconfigure the driver.
        reply = self._dvr_client.cmd_dvr('initialize')
        
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

    def test_autosample(self):
        """
        Test autosample mode.
        """
        
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('configure', COMMS_CONFIG)

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('connect')

        # Test the driver is in unknown state.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('discover')

        # Test the driver is in command mode.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)
        
        # Make sure the device parameters are set to sample frequently.
        params = {
            SBE37Parameter.NAVG : 1,
            SBE37Parameter.INTERVAL : 5
        }
        reply = self._dvr_client.cmd_dvr('set', params)
        
        reply = self._dvr_client.cmd_dvr('execute_start_autosample')

        # Test the driver is in autosample mode.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.AUTOSAMPLE)
        
        # Wait for a few samples to roll in.
        time.sleep(30)
        
        # Return to command mode. Catch timeouts and retry if necessary.
        count = 0
        while True:
            try:
                reply = self._dvr_client.cmd_dvr('execute_stop_autosample')
            
            except TimeoutError:
                count += 1
                if count >= 5:
                    self.fail('Could not wakeup device to leave autosample mode.')

            else:
                break

        # Test the driver is in command mode.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        # Verify we received at least 2 samples.
        sample_events = [evt for evt in self._events if evt['type']==DriverAsyncEvent.SAMPLE]
        self.assertTrue(len(sample_events) >= 2)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('disconnect')

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Initialize the driver and transition to unconfigured.
        reply = self._dvr_client.cmd_dvr('initialize')
    
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

    # Note: the following test is probably not supported by the simulator.
    def test_test(self):
        """
        Test the hardware testing mode.
        """
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('configure', COMMS_CONFIG)

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('connect')

        # Test the driver is in unknown state.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('discover')

        # Test the driver is in command mode.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        start_time = time.time()
        reply = self._dvr_client.cmd_dvr('execute_test')

        # Test the driver is in unknown state.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.TEST)
        
        while state != SBE37ProtocolState.COMMAND:
            time.sleep(5)
            elapsed = time.time() - start_time
            mi_logger.info('Device testing %f seconds elapsed.' % elapsed)
            state = self._dvr_client.cmd_dvr('get_current_state')

        # Verify we received the test result and it passed.
        test_results = [evt for evt in self._events if evt['type']==DriverAsyncEvent.TEST_RESULT]
        self.assertTrue(len(test_results) == 1)
        self.assertEqual(test_results[0]['value']['success'], 'Passed')

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('disconnect')

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Initialize the driver and transition to unconfigured.
        reply = self._dvr_client.cmd_dvr('initialize')
    
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

    def test_errors(self):
        """
        Test response to erroneous commands and parameters.
        """
        
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Assert for an unknown driver command.
        with self.assertRaises(UnknownCommandError):
            reply = self._dvr_client.cmd_dvr('bogus_command')

        # Assert for a known command, invalid state.
        with self.assertRaises(StateError):
            reply = self._dvr_client.cmd_dvr('execute_acquire_sample')

        # Assert we forgot the comms parameter.
        with self.assertRaises(ParameterError):
            reply = self._dvr_client.cmd_dvr('configure')

        # Assert we send a bad config object (not a dict).
        with self.assertRaises(ParameterError):
            BOGUS_CONFIG = 'not a config dict'            
            reply = self._dvr_client.cmd_dvr('configure', BOGUS_CONFIG)
            
        # Assert we send a bad config object (missing addr value).
        with self.assertRaises(ParameterError):
            BOGUS_CONFIG = COMMS_CONFIG.copy()
            BOGUS_CONFIG.pop('addr')
            reply = self._dvr_client.cmd_dvr('configure', BOGUS_CONFIG)

        # Assert we send a bad config object (bad addr value).
        with self.assertRaises(ParameterError):
            BOGUS_CONFIG = COMMS_CONFIG.copy()
            BOGUS_CONFIG['addr'] = ''
            reply = self._dvr_client.cmd_dvr('configure', BOGUS_CONFIG)
        
        # Configure for comms.
        reply = self._dvr_client.cmd_dvr('configure', COMMS_CONFIG)

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Assert for a known command, invalid state.
        with self.assertRaises(StateError):
            reply = self._dvr_client.cmd_dvr('execute_acquire_sample')

        reply = self._dvr_client.cmd_dvr('connect')
                
        # Test the driver is in unknown state.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Assert for a known command, invalid state.
        with self.assertRaises(StateError):
            reply = self._dvr_client.cmd_dvr('execute_acquire_sample')
                
        reply = self._dvr_client.cmd_dvr('discover')

        # Test the driver is in command mode.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        # Poll for a sample and confirm result.
        reply = self._dvr_client.cmd_dvr('execute_acquire_sample')
        self.assertSampleDict(reply)

        # Assert for a known command, invalid state.
        with self.assertRaises(StateError):
            reply = self._dvr_client.cmd_dvr('execute_stop_autosample')
        
        # Assert for a known command, invalid state.
        with self.assertRaises(StateError):
            reply = self._dvr_client.cmd_dvr('connect')

        # Get all device parameters. Confirm all expected keys are retrived
        # and have correct type.
        reply = self._dvr_client.cmd_dvr('get', SBE37Parameter.ALL)
        self.assertParamDict(reply, True)
        
        # Assert get fails without a parameter.
        with self.assertRaises(ParameterError):
            reply = self._dvr_client.cmd_dvr('get')
            
        # Assert get fails without a bad parameter (not ALL or a list).
        with self.assertRaises(ParameterError):
            bogus_params = 'I am a bogus param list.'
            reply = self._dvr_client.cmd_dvr('get', bogus_params)
            
        # Assert get fails without a bad parameter (not ALL or a list).
        #with self.assertRaises(InvalidParameterValueError):
        with self.assertRaises(ParameterError):
            bogus_params = [
                'a bogus parameter name',
                SBE37Parameter.INTERVAL,
                SBE37Parameter.STORETIME,
                SBE37Parameter.TCALDATE
                ]
            reply = self._dvr_client.cmd_dvr('get', bogus_params)        
        
        # Assert we cannot set a bogus parameter.
        with self.assertRaises(ParameterError):
            bogus_params = {
                'a bogus parameter name' : 'bogus value'
            }
            reply = self._dvr_client.cmd_dvr('set', bogus_params)
            
        # Assert we cannot set a real parameter to a bogus value.
        with self.assertRaises(ParameterError):
            bogus_params = {
                SBE37Parameter.INTERVAL : 'bogus value'
            }
            reply = self._dvr_client.cmd_dvr('set', bogus_params)
        
        # Disconnect from the port agent.
        reply = self._dvr_client.cmd_dvr('disconnect')
        
        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)
        
        # Deconfigure the driver.
        reply = self._dvr_client.cmd_dvr('initialize')
        
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)
    
    # Note: the following test is probably not supported by the simulator.    
    def test_discover_autosample(self):
        """
        Test the device can discover autosample mode.
        """
        
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('configure', COMMS_CONFIG)

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('connect')

        # Test the driver is in unknown state.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('discover')

        # Test the driver is in command mode.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)
        
        # Make sure the device parameters are set to sample frequently.
        params = {
            SBE37Parameter.NAVG : 1,
            SBE37Parameter.INTERVAL : 5
        }
        reply = self._dvr_client.cmd_dvr('set', params)
        
        reply = self._dvr_client.cmd_dvr('execute_start_autosample')

        # Test the driver is in autosample mode.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.AUTOSAMPLE)
    
        # Let a sample or two come in.
        gevent.sleep(15)
    
        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('disconnect')

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Initialize the driver and transition to unconfigured.
        reply = self._dvr_client.cmd_dvr('initialize')
    
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Wait briefly before we restart the comms.
        gevent.sleep(5)
    
        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('configure', COMMS_CONFIG)

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('connect')

        # Test the driver is in unknown state.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Configure driver for comms and transition to disconnected.
        count = 0
        while True:
            try:        
                reply = self._dvr_client.cmd_dvr('discover')

            except TimeoutError:
                count += 1
                if count >=5:
                    self.fail('Could not discover device state.')

            else:
                break

        # Test the driver is in command mode.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.AUTOSAMPLE)

        # Let a sample or two come in.
        # This device takes awhile to begin transmitting again after you
        # prompt it in autosample mode.
        gevent.sleep(15)

        # Return to command mode. Catch timeouts and retry if necessary.
        count = 0
        while True:
            try:
                reply = self._dvr_client.cmd_dvr('execute_stop_autosample')
            
            except TimeoutError:
                count += 1
                if count >= 5:
                    self.fail('Could not wakeup device to leave autosample mode.')

            else:
                break

        # Test the driver is in command mode.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        # Configure driver for comms and transition to disconnected.
        reply = self._dvr_client.cmd_dvr('disconnect')

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Initialize the driver and transition to unconfigured.
        reply = self._dvr_client.cmd_dvr('initialize')
    
        # Test the driver is in state unconfigured.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.UNCONFIGURED)
        
        
        