#!/usr/bin/env python

"""
@package ion.services.mi.test.test_sbe37_driver
@file ion/services/mi/test_sbe37_driver.py
@author Edward Hunter
@brief Test cases for SBE37Driver
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

from gevent import monkey; monkey.patch_all()

import time
import unittest
import logging
from subprocess import Popen
import os
import signal

from nose.plugins.attrib import attr

from pyon.util.unit_test import PyonTestCase

from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess
from ion.services.mi.drivers.sbe37_driver import SBE37Channel
from ion.services.mi.drivers.sbe37_driver import SBE37Parameter
from ion.services.mi.drivers.sbe37_driver import SBE37Command
from ion.services.mi.drivers.sbe37_driver import SBE37Driver
from ion.services.mi.common import InstErrorCode
import ion.services.mi.mi_logger

mi_logger = logging.getLogger('mi_logger')

#from pyon.public import log

# Make tests verbose and provide stdout
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_get_set
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_config
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_check_args
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_connect
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_poll
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_autosample


@unittest.skip('Do not run hardware test.')
@attr('UNIT', group='mi')
class TestSBE37Driver(PyonTestCase):    
    """
    Unit tests for ZMQ driver process.
    """
    def setUp(self):
        """
        Setup test cases.
        """
        # Zmq parameters used by driver process and client.
        self.server_addr = 'localhost'
        self.cmd_port = 5556
        self.evt_port = 5557
        
        # Driver module parameters.
        self.dvr_mod = 'ion.services.mi.drivers.sbe37_driver'
        self.dvr_cls = 'SBE37Driver'

        # Comms config.
        self.comms_config = {
            'method':'ethernet',
            'device_addr': '137.110.112.119',
            'device_port': 4001,
            'server_addr': 'localhost',
            'server_port': 8888            
        }

        # Add cleanup handler functions.
        # self.addCleanup()
        
        self.events = None
        
    def clear_events(self):
        """
        """
        self.events = []
        
    def evt_recd(self, evt):
        self.events.append(evt)
    
    def test_process(self):
        """
        """
        # Launch driver process.
        driver_process = ZmqDriverProcess.launch_process(self.cmd_port,
            self.evt_port, self.dvr_mod,  self.dvr_cls)
        
        # Create client and start messaging.
        driver_client = ZmqDriverClient(self.server_addr, self.cmd_port,
                                        self.evt_port)
        
        self.clear_events()
        driver_client.start_messaging(self.evt_recd)
        time.sleep(2)

        # Add test to verify process exists.

        # Send a test message to the process interface, confirm result.
        msg = 'I am a ZMQ message going to the process.'
        reply = driver_client.cmd_dvr('process_echo', msg)
        self.assertEqual(reply,'process_echo: '+msg)

        # Send a test message to the driver interface, confirm result.
        msg = 'I am a ZMQ message going to the driver.'
        reply = driver_client.cmd_dvr('driver_echo', msg)
        self.assertEqual(reply, 'driver_echo: '+msg)
        
        # Test the event thread publishes and client side picks up events.
        events = [
            'I am important event #1!',
            'And I am important event #2!'
            ]
        reply = driver_client.cmd_dvr('test_events', events=events)
        time.sleep(2)
        
        # Confirm the events received are as expected.
        self.assertEqual(self.events, events)
        
        # Terminate driver process and stop client messaging.
        driver_client.done()
        driver_process.wait()
        
        # Add test to verify process does not exist.
    
    def test_config(self):
        """
        """
        # Launch driver process.
        driver_process = ZmqDriverProcess.launch_process(self.cmd_port,
            self.evt_port, self.dvr_mod,  self.dvr_cls)
        
        # Create client and start messaging.
        driver_client = ZmqDriverClient(self.server_addr, self.cmd_port,
                                        self.evt_port)
        driver_client.start_messaging()
        time.sleep(2)

        configs = {SBE37Channel.CTD:self.comms_config}
        reply = driver_client.cmd_dvr('configure', configs)
        time.sleep(2)

        reply = driver_client.cmd_dvr('initialize', [SBE37Channel.CTD])
        time.sleep(2)
        
        # Terminate driver process and stop client messaging.
        driver_client.done()
        driver_process.wait()
    
    def test_connect(self):
        """
        """
        # Launch driver process.
        driver_process = ZmqDriverProcess.launch_process(self.cmd_port,
            self.evt_port, self.dvr_mod,  self.dvr_cls)
        
        # Create client and start messaging.
        driver_client = ZmqDriverClient(self.server_addr, self.cmd_port,
                                        self.evt_port)
        driver_client.start_messaging()
        time.sleep(2)

        configs = {SBE37Channel.CTD:self.comms_config}
        reply = driver_client.cmd_dvr('configure', configs)
        time.sleep(2)

        reply = driver_client.cmd_dvr('connect', [SBE37Channel.CTD])
        time.sleep(2)
                
        reply = driver_client.cmd_dvr('disconnect', [SBE37Channel.CTD])
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('initialize', [SBE37Channel.CTD])
        time.sleep(2)

        # Terminate driver process and stop client messaging.
        driver_client.done()
        driver_process.wait()        
        
    def test_get_set(self):
        """
        """
        # Typical factory settings for tested paramters.
        """
        TA2=-4.858579e-06
        PTCA1=-0.6603433
        TCALDATE=(8, 11, 2005)        
        """
        
        # Launch driver process.
        driver_process = ZmqDriverProcess.launch_process(self.cmd_port,
            self.evt_port, self.dvr_mod,  self.dvr_cls)

        # Create client and start messaging.        
        driver_client = ZmqDriverClient(self.server_addr, self.cmd_port,
                                        self.evt_port)
        driver_client.start_messaging()
        time.sleep(3)

        # Configure driver.
        configs = {SBE37Channel.CTD:self.comms_config}
        reply = driver_client.cmd_dvr('configure', configs)
        time.sleep(2)

        # Connect driver.
        reply = driver_client.cmd_dvr('connect', [SBE37Channel.CTD])
        time.sleep(2)
        
        # Get all parameters.
        get_params = [
            (SBE37Channel.CTD, SBE37Parameter.ALL)            
        ]
        reply = driver_client.cmd_dvr('get', get_params)
        success = reply[0]
        result = reply[1]
        time.sleep(2)
        
        # Check overall and individual parameter success. Check parameter types.
        self.assert_(InstErrorCode.is_ok(success))
        self.assertIsInstance(result[(SBE37Channel.CTD, SBE37Parameter.TA2)], float)
        self.assertIsInstance(result[(SBE37Channel.CTD, SBE37Parameter.PTCA1)], float)
        self.assertIsInstance(result[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)], tuple)
        
        # Set up a param dict of the original values.
        old_ta2 = result[(SBE37Channel.CTD, SBE37Parameter.TA2)]
        old_ptca1 = result[(SBE37Channel.CTD, SBE37Parameter.PTCA1)]
        old_tcaldate = result[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)]
               
        orig_params = {
            (SBE37Channel.CTD, SBE37Parameter.TA2): old_ta2,
            (SBE37Channel.CTD, SBE37Parameter.PTCA1): old_ptca1,
            (SBE37Channel.CTD, SBE37Parameter.TCALDATE): old_tcaldate            
        }

        # Set up a param dict of new values.
        new_ta2 = old_ta2*2
        new_ptcal1 = old_ptca1*2
        new_tcaldate = (1, 1, 2011)
        
        new_params = {
            (SBE37Channel.CTD, SBE37Parameter.TA2): new_ta2,
            (SBE37Channel.CTD, SBE37Parameter.PTCA1): new_ptcal1,
            (SBE37Channel.CTD, SBE37Parameter.TCALDATE): new_tcaldate
        }
        
        # Set the params to their new values.
        reply = driver_client.cmd_dvr('set', new_params)
        success = reply[0]
        result = reply[1]        
        time.sleep(2)
        
        # Check overall success and success of the individual paramters.
        self.assert_(InstErrorCode.is_ok(success))
        
        # Get the same paramters back from the driver.
        get_params = [
            (SBE37Channel.CTD, SBE37Parameter.TA2),
            (SBE37Channel.CTD, SBE37Parameter.PTCA1),
            (SBE37Channel.CTD, SBE37Parameter.TCALDATE)
        ]
        reply = driver_client.cmd_dvr('get', get_params)
        success = reply[0]
        result = reply[1]        
        time.sleep(2)

        # Check success, and check that the parameters were set to the
        # new values.
        self.assert_(InstErrorCode.is_ok(success))
        self.assertIsInstance(result[(SBE37Channel.CTD, SBE37Parameter.TA2)], float)
        self.assertIsInstance(result[(SBE37Channel.CTD, SBE37Parameter.PTCA1)], float)
        self.assertIsInstance(result[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)], tuple)
        self.assertAlmostEqual(result[(SBE37Channel.CTD, SBE37Parameter.TA2)], new_ta2, delta=abs(0.01*new_ta2))
        self.assertAlmostEqual(result[(SBE37Channel.CTD, SBE37Parameter.PTCA1)], new_ptcal1, delta=abs(0.01*new_ptcal1))
        self.assertEqual(result[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)], new_tcaldate)

        # Set the paramters back to their original values.        
        reply = driver_client.cmd_dvr('set', orig_params)
        success = reply[0]
        result = reply[1]
        self.assert_(InstErrorCode.is_ok(success))

        # Get the parameters back from the driver.
        reply = driver_client.cmd_dvr('get', get_params)
        success = reply[0]
        result = reply[1]        

        # Check overall and individual sucess, and that paramters were
        # returned to their original values.
        self.assert_(InstErrorCode.is_ok(success))
        self.assertIsInstance(result[(SBE37Channel.CTD, SBE37Parameter.TA2)], float)
        self.assertIsInstance(result[(SBE37Channel.CTD, SBE37Parameter.PTCA1)], float)
        self.assertIsInstance(result[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)], tuple)
        self.assertAlmostEqual(result[(SBE37Channel.CTD, SBE37Parameter.TA2)], old_ta2, delta=abs(0.01*old_ta2))
        self.assertAlmostEqual(result[(SBE37Channel.CTD, SBE37Parameter.PTCA1)], old_ptca1, delta=abs(0.01*old_ptca1))
        self.assertEqual(result[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)], old_tcaldate)
        
        # Disconnect driver from the device.        
        reply = driver_client.cmd_dvr('disconnect', [SBE37Channel.CTD])
        time.sleep(2)
        
        # Deconfigure the driver.
        reply = driver_client.cmd_dvr('initialize', [SBE37Channel.CTD])
        time.sleep(2)
        
        # End driver process and client messaging.
        driver_client.done()
        driver_process.wait()

    def test_poll(self):
        """
        """
        # Launch driver process.
        driver_process = ZmqDriverProcess.launch_process(self.cmd_port,
            self.evt_port, self.dvr_mod,  self.dvr_cls)
        
        # Create client and start messaging.
        driver_client = ZmqDriverClient(self.server_addr, self.cmd_port,
                                        self.evt_port)
        self.clear_events()
        driver_client.start_messaging(self.evt_recd)
        time.sleep(2)

        configs = {SBE37Channel.CTD:self.comms_config}
        reply = driver_client.cmd_dvr('configure', configs)
        time.sleep(2)

        reply = driver_client.cmd_dvr('connect', [SBE37Channel.CTD])
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('execute', [SBE37Channel.CTD], [SBE37Command.ACQUIRE_SAMPLE])
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('execute', [SBE37Channel.CTD], [SBE37Command.ACQUIRE_SAMPLE])
        time.sleep(2)

        reply = driver_client.cmd_dvr('execute', [SBE37Channel.CTD], [SBE37Command.ACQUIRE_SAMPLE])
        time.sleep(2)

        print 'EVENTS RECEIVED:'
        print str(self.events)

        reply = driver_client.cmd_dvr('disconnect', [SBE37Channel.CTD])
        time.sleep(2)
        
        # Deconfigure the driver.
        reply = driver_client.cmd_dvr('initialize', [SBE37Channel.CTD])
        time.sleep(2)
        
        # Terminate driver process and stop client messaging.
        driver_client.done()
        driver_process.wait()
    
    def test_autosample(self):
        """
        """
        # Launch driver process.
        driver_process = ZmqDriverProcess.launch_process(self.cmd_port,
            self.evt_port, self.dvr_mod,  self.dvr_cls)
        
        # Create client and start messaging.
        driver_client = ZmqDriverClient(self.server_addr, self.cmd_port,
                                        self.evt_port)
        driver_client.start_messaging()
        time.sleep(2)

        configs = {SBE37Channel.CTD:self.comms_config}
        reply = driver_client.cmd_dvr('configure', configs)
        time.sleep(2)

        reply = driver_client.cmd_dvr('connect', [SBE37Channel.CTD])
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('execute', [SBE37Channel.CTD], [SBE37Command.START_AUTO_SAMPLING])
        time.sleep(30)
        
        tries = 10
        count = 0
        while count < tries:
            reply = driver_client.cmd_dvr('execute', [SBE37Channel.CTD], [SBE37Command.STOP_AUTO_SAMPLING])
            if InstErrorCode.is_ok(reply[0]):
                break
            time.sleep(2)
        time.sleep(2)

        reply = driver_client.cmd_dvr('disconnect', [SBE37Channel.CTD])
        time.sleep(2)
        
        # Deconfigure the driver.
        reply = driver_client.cmd_dvr('initialize', [SBE37Channel.CTD])
        time.sleep(2)
        
        # Terminate driver process and stop client messaging.
        driver_client.done()
        driver_process.wait()
        
    """
    def test_check_args(self):
        
        
        
        c1 = [SBE37Channel.ALL]
        c2 = [SBE37Channel.CTD]
        c3 = [SBE37Channel.CTD, SBE37Channel.CTD]
        c4 = [SBE37Channel.CTD, SBE37Channel.ALL]
        c5 = ['bogus1', 'bogus2', SBE37Channel.CTD, SBE37Channel.ALL]
        
        result = SBE37Driver._check_channel_args(c1)
        print str(result)
        result = SBE37Driver._check_channel_args(c2)
        print str(result)
        result = SBE37Driver._check_channel_args(c3)
        print str(result)
        result = SBE37Driver._check_channel_args(c4)
        print str(result)
        result = SBE37Driver._check_channel_args(c5)
        print str(result)
        
        g1 = [(SBE37Channel.CTD, SBE37Parameter.TA0)]
        g2 = [(SBE37Channel.ALL, SBE37Parameter.TA0)]
        g3 = [(SBE37Channel.ALL, SBE37Parameter.ALL)]
        g4 = [(SBE37Channel.ALL, SBE37Parameter.CH),
                    (SBE37Channel.ALL, SBE37Parameter.TA0),
                    (SBE37Channel.ALL, SBE37Parameter.OUTPUTSAL)]
        g5 = [(SBE37Channel.CTD, SBE37Parameter.CH),
                    (SBE37Channel.CTD, SBE37Parameter.TA0),
                    (SBE37Channel.ALL, SBE37Parameter.OUTPUTSAL)]
        g6 = [('bogus', SBE37Parameter.CH),
                    (SBE37Channel.CTD, 'bogus'),
                    (SBE37Channel.ALL, SBE37Parameter.OUTPUTSAL)]
        
        result = SBE37Driver._check_get_args(g1)
        print str(result)
        result = SBE37Driver._check_get_args(g2)
        print str(result)
        result = SBE37Driver._check_get_args(g3)
        print str(result)
        result = SBE37Driver._check_get_args(g4)
        print str(result)
        result = SBE37Driver._check_get_args(g5)
        print str(result)
        result = SBE37Driver._check_get_args(g6)
        print str(result)
        
        s1 = {
            (SBE37Channel.CTD, SBE37Parameter.CH) : 0.95,
            (SBE37Channel.CTD, SBE37Parameter.TA0) : 0.95,
            (SBE37Channel.CTD, SBE37Parameter.OUTPUTSAL) : True            
        }
        s2 = {
            (SBE37Channel.ALL, SBE37Parameter.CH) : 0.95,
            (SBE37Channel.ALL, SBE37Parameter.TA0) : 0.95,
            (SBE37Channel.ALL, SBE37Parameter.OUTPUTSAL) : True            
        }
        s3 = {
            (SBE37Channel.ALL, SBE37Parameter.ALL) : 0.95,
            (SBE37Channel.ALL, SBE37Parameter.TA0) : 0.95,
            (SBE37Channel.ALL, SBE37Parameter.OUTPUTSAL) : True            
        }
        s4 = {
            (SBE37Channel.ALL, 'bogus') : 0.95,
            ('bogus', SBE37Parameter.TA0) : 0.95,
            (SBE37Channel.ALL, SBE37Parameter.OUTPUTSAL) : True            
        }
        
        result = SBE37Driver._check_set_args(s1)
        print str(result)
        result = SBE37Driver._check_set_args(s2)
        print str(result)
        result = SBE37Driver._check_set_args(s3)
        print str(result)
        result = SBE37Driver._check_set_args(s4)
        print str(result)
        """


    
