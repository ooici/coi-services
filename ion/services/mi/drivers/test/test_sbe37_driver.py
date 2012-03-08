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
from pyon.public import CFG

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
from mock import patch

mi_logger = logging.getLogger('mi_logger')

#from pyon.public import log

# Make tests verbose and provide stdout
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_get_set
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_config
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_connect
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_poll
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_autosample

#@unittest.skip('Do not run hardware test.')
@attr('HARDWARE', group='mi')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestSBE37Driver(PyonTestCase):
    _driver_client = None
    _driver_process = None
    
    """
    Integration tests for the sbe37 driver. This class tests and shows
    use patterns for the sbe37 driver as a zmq driver process.
    """
    def setUp(self):
        """
        Setup test cases.
        """
        # Zmq parameters to configure communications with the driver process.
        self.server_addr = 'localhost'
        self.cmd_port = 5556
        self.evt_port = 5557
        
        # Driver module parameters for importing and constructing the driver.
        self.dvr_mod = 'ion.services.mi.drivers.sbe37_driver'
        self.dvr_cls = 'SBE37Driver'

        # Driver comms config. This is passed as a configure message
        # argument to transition the driver to disconnected and ready to
        # connect.
        self.comms_config = {
            SBE37Channel.CTD:{
                'method':'ethernet',
                'device_addr': CFG.device.sbe37.host,
                'device_port': CFG.device.sbe37.port,
                'server_addr': 'localhost',
                'server_port': 8888
            }
        }

        # Add cleanup handler functions.
        # Add functions to detect and kill processes and remove pidfiles
        # as necessary.
        #psout = subprocess.check_output(['ps -e | grep python'], shell=True)
        #1724 ??         0:00.01 /Users/edwardhunter/Documents/Dev/virtenvs/coi/bin/python bin/python -c import ion.services.mi.logger_process as lp; l = lp.EthernetD
        #1721 ttys000    0:00.24 /Users/edwardhunter/Documents/Dev/virtenvs/coi/bin/python bin/python -c from ion.services.mi.zmq_driver_process import ZmqDriverProce
        #
        #1742 ??         0:00.01 /Users/edwardhunter/Documents/Dev/virtenvs/coi/bin/python bin/python -c import ion.services.mi.logger_process as lp; l = lp.EthernetDeviceLogger("137.110.112.119", 4001, 8888, "/", ["<<",">>"]); l.start()        
        #1739 ttys000    0:02.66 /Users/edwardhunter/Documents/Dev/virtenvs/coi/bin/python bin/python -c from ion.services.mi.zmq_driver_process import ZmqDriverProcess; dp = ZmqDriverProcess(5556, 5557, "ion.services.mi.drivers.sbe37_driver", "SBE37Driver");dp.run()
        # self.addCleanup()
        
        self.events = None
        
    def init_comms(self):
        """
        Setup driver process and client
        """
        if( not self._driver_client and not self._driver_process ):
            # Launch driver process.
            self._driver_process = ZmqDriverProcess.launch_process(self.cmd_port,
                        self.evt_port, self.dvr_mod,  self.dvr_cls)
        
            # Create client and start messaging.
            self._driver_client = ZmqDriverClient(self.server_addr, self.cmd_port,
                                                    self.evt_port)
            self._driver_client.start_messaging()
            time.sleep(2)
        
        self.clear_events()
        return (self._driver_process, self._driver_client)
        
    def tearDown(self):
        mi_logger.info("Tear down test case.")
        if(self._driver_client and self._driver_process):
            self._driver_client.done()
            self._driver_process.wait()
            
        else:
            raise Exception("No client")
            
        
    def clear_events(self):
        """
        Clear the event list.
        """
        self.events = []
        
    def evt_recd(self, evt):
        """
        Simple callback to catch events from the driver for verification.
        """
        self.events.append(evt)
    
    def test_process(self):
        """
        Test for correct launch of driver process and communications, including
        asynchronous driver events.
        """
        # Launch driver process.
        driver_process, driver_client = self.init_comms()

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
    
    
    def test_config(self):
        """
        Test to configure the driver process for device comms and transition
        to disconnected state.
        """
        # Launch driver process.
        driver_process, driver_client = self.init_comms()

        # Configure driver for comms and transition to disconnected.
        reply = driver_client.cmd_dvr('configure', self.comms_config)
        time.sleep(2)

        # Initialize the driver and transition to unconfigured.
        reply = driver_client.cmd_dvr('initialize')
        time.sleep(2)
        
    
    def test_connect(self):
        """
        Test to establish device comms and transition to command state.
        """
        # Launch driver process.
        driver_process, driver_client = self.init_comms()

        # Configure driver for comms and transition to disconnected.
        reply = driver_client.cmd_dvr('configure', self.comms_config)
        time.sleep(2)

        # Establish device comms and transition to command.
        reply = driver_client.cmd_dvr('connect')
        time.sleep(2)

        # Disconnect devcie comms and transition to disconnected.                
        reply = driver_client.cmd_dvr('disconnect')
        time.sleep(2)
        
        # Initialize driver and transition to unconfigured.
        reply = driver_client.cmd_dvr('initialize')
        time.sleep(2)

        
    def test_get_set(self):
        """
        Test driver parameter get/set interface including device persistence.
        TA2=-4.858579e-06
        PTCA1=-0.6603433
        TCALDATE=(8, 11, 2005)        
        """
        
        # Launch driver process.
        driver_process, driver_client = self.init_comms()

        # Configure driver for comms and transition to disconnected.
        reply = driver_client.cmd_dvr('configure', self.comms_config)
        time.sleep(2)

        # Establish devcie comms and transition to command.
        reply = driver_client.cmd_dvr('connect')
        time.sleep(2)
        
        # Get all parameters.
        get_params = [
            (SBE37Channel.CTD, SBE37Parameter.ALL)            
        ]
        reply = driver_client.cmd_dvr('get', get_params)
        time.sleep(2)
        
        # Check overall and individual parameter success. Check parameter types.
        self.assertIsInstance(reply, dict)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.TA2)],
                                                            float)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.PTCA1)],
                                                            float)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)],
                                                            (list, tuple))
        
        # Set up a param dict of the original values.
        old_ta2 = reply[(SBE37Channel.CTD, SBE37Parameter.TA2)]
        old_ptca1 = reply[(SBE37Channel.CTD, SBE37Parameter.PTCA1)]
        old_tcaldate = reply[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)]
        orig_params = {
            (SBE37Channel.CTD, SBE37Parameter.TA2): old_ta2,
            (SBE37Channel.CTD, SBE37Parameter.PTCA1): old_ptca1,
            (SBE37Channel.CTD, SBE37Parameter.TCALDATE): old_tcaldate            
        }

        # Set up a param dict of new values.
        new_ta2 = old_ta2*2
        new_ptcal1 = old_ptca1*2
        new_tcaldate = list(old_tcaldate)
        new_tcaldate[2] = new_tcaldate[2] + 1
        new_tcaldate = tuple(new_tcaldate)
        new_params = {
            (SBE37Channel.CTD, SBE37Parameter.TA2): new_ta2,
            (SBE37Channel.CTD, SBE37Parameter.PTCA1): new_ptcal1,
            (SBE37Channel.CTD, SBE37Parameter.TCALDATE): new_tcaldate
        }
        
        # Set the params to their new values.
        reply = driver_client.cmd_dvr('set', new_params)
        time.sleep(2)
        
        # Check overall success and success of the individual paramters.
        self.assertIsInstance(reply, dict)
        mi_logger.debug('set result: %s', str(reply))
        
        # Get the same paramters back from the driver.
        get_params = [
            (SBE37Channel.CTD, SBE37Parameter.TA2),
            (SBE37Channel.CTD, SBE37Parameter.PTCA1),
            (SBE37Channel.CTD, SBE37Parameter.TCALDATE)
        ]
        reply = driver_client.cmd_dvr('get', get_params)
        time.sleep(2)

        # Check success, and check that the parameters were set to the
        # new values.
        self.assertIsInstance(reply, dict)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.TA2)],
                                                            float)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.PTCA1)],
                                                            float)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)],
                                                            (list, tuple))
        self.assertAlmostEqual(reply[(SBE37Channel.CTD, SBE37Parameter.TA2)],
                                    new_ta2, delta=abs(0.01*new_ta2))
        self.assertAlmostEqual(reply[(SBE37Channel.CTD, SBE37Parameter.PTCA1)],
                                    new_ptcal1, delta=abs(0.01*new_ptcal1))
        self.assertEqual(reply[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)],
                                                            new_tcaldate)


        # Set the paramters back to their original values.        
        reply = driver_client.cmd_dvr('set', orig_params)
        self.assertIsInstance(reply, dict)
        mi_logger.debug('set result: %s', str(reply))

        # Get the parameters back from the driver.
        reply = driver_client.cmd_dvr('get', get_params)

        # Check overall and individual sucess, and that paramters were
        # returned to their original values.
        self.assertIsInstance(reply, dict)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.TA2)],
                                                    float)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.PTCA1)],
                                                    float)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)],
                                                    (list, tuple))
        self.assertAlmostEqual(reply[(SBE37Channel.CTD, SBE37Parameter.TA2)],
                                            old_ta2, delta=abs(0.01*old_ta2))
        self.assertAlmostEqual(reply[(SBE37Channel.CTD, SBE37Parameter.PTCA1)],
                                        old_ptca1, delta=abs(0.01*old_ptca1))
        self.assertEqual(reply[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)],
                                        old_tcaldate)
        
        # Disconnect driver from the device and transition to disconnected.
        reply = driver_client.cmd_dvr('disconnect', [SBE37Channel.CTD])
        time.sleep(2)
        
        # Deconfigure the driver and transition to unconfigured.
        reply = driver_client.cmd_dvr('initialize', [SBE37Channel.CTD])
        time.sleep(2)
        
        # End driver process and client messaging.
        driver_client.done()
        driver_process.wait()

    def test_poll(self):
        """
        Test sample polling commands and events.
        """
        # Launch driver process.
        driver_process, driver_client = self.init_comms()

        reply = driver_client.cmd_dvr('configure', self.comms_config)
        time.sleep(2)

        reply = driver_client.cmd_dvr('connect')
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('get_active_channels')
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('execute_acquire_sample')
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('execute_acquire_sample')
        time.sleep(2)

        reply = driver_client.cmd_dvr('execute_acquire_sample')
        time.sleep(2)

        print 'EVENTS RECEIVED:'
        print str(self.events)

        reply = driver_client.cmd_dvr('disconnect')
        time.sleep(2)
        
        # Deconfigure the driver.
        reply = driver_client.cmd_dvr('initialize')
        time.sleep(2)
        
    
    def test_autosample(self):
        """
        Test autosample command and state, including events.
        """
        # Launch driver process.
        driver_process, driver_client = self.init_comms()

        reply = driver_client.cmd_dvr('configure', self.comms_config)
        time.sleep(2)

        reply = driver_client.cmd_dvr('connect')
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('start_autosample')
        time.sleep(30)
        
        while True:
            reply = driver_client.cmd_dvr('stop_autosample')
            if not reply[SBE37Channel.CTD]:
                break
            time.sleep(2)
        time.sleep(2)

        reply = driver_client.cmd_dvr('disconnect')
        time.sleep(2)
        
        # Deconfigure the driver.
        reply = driver_client.cmd_dvr('initialize')
        time.sleep(2)
        
        


    
