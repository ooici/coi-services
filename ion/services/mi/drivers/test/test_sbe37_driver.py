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
from ion.services.mi.drivers.sbe37_driver import SBE37Command
import ion.services.mi.mi_logger

mi_logger = logging.getLogger('mi_logger')

#from pyon.public import log

# Make tests verbose and provide stdout
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py

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
        self.host = 'localhost'
        self.cmd_port = 5556
        self.evt_port = 5557
        
        # Driver module parameters.
        self.dvr_mod = 'ion.services.mi.drivers.sbe37_driver'
        self.dvr_cls = 'SBE37Driver'

        #
        self.server_addr = 'localhost'

        # Add cleanup handler functions.
        # self.addCleanup()
        
    def test_config(self):
        """
        Test driver configure.
        """
        #def wait_on_child(signum=None, frame=None):
        #    retval = os.wait()
        #signal.signal(signal.SIGCHLD, wait_on_child)


        driver_process = ZmqDriverProcess.launch_process(self.cmd_port,
            self.evt_port, self.dvr_mod,  self.dvr_cls)
        
        
        driver_client = ZmqDriverClient(self.server_addr, self.cmd_port,
                                        self.evt_port)
        driver_client.start_messaging()
        time.sleep(3)
        config = {
            'method':'ethernet',
            'device_addr': '137.110.112.119',
            'device_port': 4001,
            'server_addr': 'localhost',
            'server_port': 8888            
        }
        configs = {SBE37Channel.CTD:config}
        reply = driver_client.cmd_dvr('configure', configs)
 
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('connect', [SBE37Channel.CTD])
        time.sleep(2)

        reply = driver_client.cmd_dvr('execute', [SBE37Channel.CTD], [SBE37Command.ACQUIRE_SAMPLE])
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('disconnect', [SBE37Channel.CTD])
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('initialize', [SBE37Channel.CTD])
        time.sleep(2)
        
        
        driver_client.done()
        time.sleep(2)
        driver_process.wait()
        time.sleep(2)

        """
        reply = driver_client.cmd_dvr('process_echo', data='test 1 2 3')
        self.assertIsInstance(reply, dict)
        self.assertTrue('cmd' in reply)
        self.assertTrue('args' in reply)
        self.assertTrue('kwargs' in reply)
        self.assertTrue(reply['cmd'] == 'process_echo')
        self.assertTrue(reply['args'] == ())
        self.assertIsInstance(reply['kwargs'], dict)
        self.assertTrue('data' in reply['kwargs'])
        self.assertTrue(reply['kwargs']['data'], 'test 1 2 3')

        reply = driver_client.cmd_dvr('process_echo',
                                      data='zoom zoom boom boom')
        self.assertIsInstance(reply, dict)
        self.assertTrue('cmd' in reply)
        self.assertTrue('args' in reply)
        self.assertTrue('kwargs' in reply)
        self.assertTrue(reply['cmd'] == 'process_echo')
        self.assertTrue(reply['args'] == ())
        self.assertIsInstance(reply['kwargs'], dict)
        self.assertTrue('data' in reply['kwargs'])
        self.assertTrue(reply['kwargs']['data'], 'test 1 2 3')

        events = ['I am event number 1!', 'And I am event number 2!']
        reply = driver_client.cmd_dvr('test_events', events=events)
        self.assertEqual(reply, 'test_events')
        time.sleep(3)
        self.assertTrue(driver_client.events, events)
        """
        pass
    
