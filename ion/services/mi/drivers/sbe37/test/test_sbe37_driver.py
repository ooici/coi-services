#!/usr/bin/env python

"""
@package ion.services.mi.drivers.sbe37.test.test_sbe37_driver
@file ion/services/mi/drivers/sbe37/test/test_sbe37_driver.py
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
from ion.services.mi.driver_int_test_support import DriverIntegrationTestSupport
from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess
from ion.services.mi.drivers.sbe37.sbe37_driver import SBE37Driver
from ion.services.mi.drivers.sbe37.sbe37_driver import SBE37ProtocolState
from ion.services.mi.drivers.sbe37.sbe37_driver import SBE37Parameter
from ion.services.mi.instrument_driver import DriverAsyncEvent
from ion.services.mi.instrument_driver import DriverConnectionState
from ion.services.mi.logger_process import EthernetDeviceLogger
from ion.services.mi.exceptions import InstrumentException
from ion.services.mi.exceptions import NotImplementedException
from ion.services.mi.exceptions import InstrumentTimeoutException
from ion.services.mi.exceptions import InstrumentProtocolException
from ion.services.mi.exceptions import InstrumentParameterException
from ion.services.mi.exceptions import SampleException
from ion.services.mi.exceptions import InstrumentStateException
from ion.services.mi.exceptions import InstrumentCommandException


# MI logger
import ion.services.mi.mi_logger
mi_logger = logging.getLogger('mi_logger')

# Make tests verbose and provide stdout
# bin/nosetests -s -v ion/services/mi/drivers/sbe37/test/test_sbe37_driver.py:TestSBE37Driver.test_process
# bin/nosetests -s -v ion/services/mi/drivers/sbe37/test/test_sbe37_driver.py:TestSBE37Driver.test_config
# bin/nosetests -s -v ion/services/mi/drivers/sbe37/test/test_sbe37_driver.py:TestSBE37Driver.test_connect
# bin/nosetests -s -v ion/services/mi/drivers/sbe37/test/test_sbe37_driver.py:TestSBE37Driver.test_get_set
# bin/nosetests -s -v ion/services/mi/drivers/sbe37/test/test_sbe37_driver.py:TestSBE37Driver.test_poll
# bin/nosetests -s -v ion/services/mi/drivers/sbe37/test/test_sbe37_driver.py:TestSBE37Driver.test_autosample
# bin/nosetests -s -v ion/services/mi/drivers/sbe37/test/test_sbe37_driver.py:TestSBE37Driver.test_test
# bin/nosetests -s -v ion/services/mi/drivers/sbe37/test/test_sbe37_driver.py:TestSBE37Driver.test_errors
# bin/nosetests -s -v ion/services/mi/drivers/sbe37/test/test_sbe37_driver.py:TestSBE37Driver.test_discover_autosample

DVR_MOD = 'ion.services.mi.drivers.sbe37.sbe37_driver'

DVR_CLS = 'SBE37Driver'
#DEV_ADDR = '67.58.49.220' 
#DEV_ADDR = '137.110.112.119' # Moxa DHCP in Edward's office.
DEV_ADDR = 'sbe37-simulator.oceanobservatories.org' # Simulator addr.
DEV_PORT = 4001 # Moxa port or simulator random data.
#DEV_PORT = 4002 # Simulator sine data.
WORK_DIR = '/tmp/'
DELIM = ['<<','>>']
COMMS_CONFIG = {
    'addr': 'localhost',
}

# Used to validate param config retrieved from driver.
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
#@unittest.skip('Ready to go, remove skip when tested against simulator.')
class TestSBE37Driver(unittest.TestCase):    
    """
    Integration tests for the sbe37 driver. This class tests and shows
    use patterns for the sbe37 driver as a zmq driver process.
    """    
    """
    def __init__(self):
        self.device_addr = DEV_ADDR
        self.device_port = DEV_PORT
        self.work_dir = WORK_DIR
        self.delim = DELIM
        
        self.driver_class = DVR_CLS
        self.driver_module = DVR_MOD
        self._support = DriverIntegrationTestSupport(self.driver_module,
                                                     self.driver_class,
                                                     self.device_addr,
                                                     self.device_port,
                                                     self.delim,
                                                     self.work_dir)
    """
    def setUp(self):
        """
        Setup test cases.
        """
        self.device_addr = DEV_ADDR
        self.device_port = DEV_PORT
        self.work_dir = WORK_DIR
        self.delim = DELIM
        
        self.driver_class = DVR_CLS
        self.driver_module = DVR_MOD
        self._support = DriverIntegrationTestSupport(self.driver_module,
                                                     self.driver_class,
                                                     self.device_addr,
                                                     self.device_port,
                                                     self.delim,
                                                     self.work_dir)

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
        COMMS_CONFIG['port'] = self._support.start_pagent()
        self.addCleanup(self._support.stop_pagent)    

        # Create and start the driver.
        self._support.start_driver()
        self.addCleanup(self._support.stop_driver)

        # Grab some variables from support that we need
        self._dvr_client = self._support._dvr_client
        self._dvr_proc = self._support._dvr_proc
        self._pagent = self._support._pagent
        self._events = self._support._events

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
        gevent.sleep(1)
        
        # Confirm the events received are as expected.
        self.assertEqual(self._events, events)

        # Test the exception mechanism.
        with self.assertRaises(InstrumentException):
            exception_str = 'Oh no, something bad happened!'
            reply = self._dvr_client.cmd_dvr('test_exceptions', exception_str)
        
        # Verify we received a driver error event.
        gevent.sleep(1)
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
        gevent.sleep(1)
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
        gevent.sleep(30)
        
        # Return to command mode. Catch timeouts and retry if necessary.
        count = 0
        while True:
            try:
                reply = self._dvr_client.cmd_dvr('execute_stop_autosample')
            
            except InstrumentTimeoutException:
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

    @unittest.skip('Not supported by simulator and very long (> 5 min).')
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
            gevent.sleep(5)
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
        with self.assertRaises(InstrumentCommandException):
            reply = self._dvr_client.cmd_dvr('bogus_command')

        # Assert for a known command, invalid state.
        with self.assertRaises(InstrumentStateException):
            reply = self._dvr_client.cmd_dvr('execute_acquire_sample')

        # Assert we forgot the comms parameter.
        with self.assertRaises(InstrumentParameterException):
            reply = self._dvr_client.cmd_dvr('configure')

        # Assert we send a bad config object (not a dict).
        with self.assertRaises(InstrumentParameterException):
            BOGUS_CONFIG = 'not a config dict'            
            reply = self._dvr_client.cmd_dvr('configure', BOGUS_CONFIG)
            
        # Assert we send a bad config object (missing addr value).
        with self.assertRaises(InstrumentParameterException):
            BOGUS_CONFIG = COMMS_CONFIG.copy()
            BOGUS_CONFIG.pop('addr')
            reply = self._dvr_client.cmd_dvr('configure', BOGUS_CONFIG)

        # Assert we send a bad config object (bad addr value).
        with self.assertRaises(InstrumentParameterException):
            BOGUS_CONFIG = COMMS_CONFIG.copy()
            BOGUS_CONFIG['addr'] = ''
            reply = self._dvr_client.cmd_dvr('configure', BOGUS_CONFIG)
        
        # Configure for comms.
        reply = self._dvr_client.cmd_dvr('configure', COMMS_CONFIG)

        # Test the driver is configured for comms.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, DriverConnectionState.DISCONNECTED)

        # Assert for a known command, invalid state.
        with self.assertRaises(InstrumentStateException):
            reply = self._dvr_client.cmd_dvr('execute_acquire_sample')

        reply = self._dvr_client.cmd_dvr('connect')
                
        # Test the driver is in unknown state.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        # Assert for a known command, invalid state.
        with self.assertRaises(InstrumentStateException):
            reply = self._dvr_client.cmd_dvr('execute_acquire_sample')
                
        reply = self._dvr_client.cmd_dvr('discover')

        # Test the driver is in command mode.
        state = self._dvr_client.cmd_dvr('get_current_state')
        self.assertEqual(state, SBE37ProtocolState.COMMAND)

        # Poll for a sample and confirm result.
        reply = self._dvr_client.cmd_dvr('execute_acquire_sample')
        self.assertSampleDict(reply)

        # Assert for a known command, invalid state.
        with self.assertRaises(InstrumentStateException):
            reply = self._dvr_client.cmd_dvr('execute_stop_autosample')
        
        # Assert for a known command, invalid state.
        with self.assertRaises(InstrumentStateException):
            reply = self._dvr_client.cmd_dvr('connect')

        # Get all device parameters. Confirm all expected keys are retrived
        # and have correct type.
        reply = self._dvr_client.cmd_dvr('get', SBE37Parameter.ALL)
        self.assertParamDict(reply, True)
        
        # Assert get fails without a parameter.
        with self.assertRaises(InstrumentParameterException):
            reply = self._dvr_client.cmd_dvr('get')
            
        # Assert get fails without a bad parameter (not ALL or a list).
        with self.assertRaises(InstrumentParameterException):
            bogus_params = 'I am a bogus param list.'
            reply = self._dvr_client.cmd_dvr('get', bogus_params)
            
        # Assert get fails without a bad parameter (not ALL or a list).
        #with self.assertRaises(InvalidParameterValueError):
        with self.assertRaises(InstrumentParameterException):
            bogus_params = [
                'a bogus parameter name',
                SBE37Parameter.INTERVAL,
                SBE37Parameter.STORETIME,
                SBE37Parameter.TCALDATE
                ]
            reply = self._dvr_client.cmd_dvr('get', bogus_params)        
        
        # Assert we cannot set a bogus parameter.
        with self.assertRaises(InstrumentParameterException):
            bogus_params = {
                'a bogus parameter name' : 'bogus value'
            }
            reply = self._dvr_client.cmd_dvr('set', bogus_params)
            
        # Assert we cannot set a real parameter to a bogus value.
        with self.assertRaises(InstrumentParameterException):
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
    
    @unittest.skip('Not supported by simulator.')
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
        gevent.sleep(30)
    
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
        gevent.sleep(10)
    
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

            except InstrumentTimeoutException:
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
        gevent.sleep(30)

        # Return to command mode. Catch timeouts and retry if necessary.
        count = 0
        while True:
            try:
                reply = self._dvr_client.cmd_dvr('execute_stop_autosample')
            
            except InstrumentTimeoutException:
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
        
        
        