#!/usr/bin/env python

"""
@package ion.services.mi.test.test_zmq_driver_process
@file ion/services/mi/test_zmq_driver_process.py
@author Edward Hunter
@brief Test cases for ZmqDriverProcess processes.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

from gevent import monkey; monkey.patch_all()

import time
import unittest
import logging
from subprocess import Popen

from nose.plugins.attrib import attr

from pyon.util.unit_test import PyonTestCase

from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess
import ion.services.mi.mi_logger

mi_logger = logging.getLogger('mi_logger')

#from pyon.public import log

# Make tests verbose and provide stdout
# bin/nosetests -s -v ion/services/mi/test/test_driver_process.py

@attr('UNIT', group='mi')
class TestZmqDriverProcess(PyonTestCase):    
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
        self.dvr_mod = 'ion.services.mi.sbe37_driver'
        self.dvr_cls = 'SBE37Driver'


        # Add cleanup handler functions.
        # self.addCleanup()

        
    def test_driver_process(self):
        """
        Test driver process launch and comms.
        """
        
        """
        driver_process = ZmqDriverProcess.launch_process(5556, 5557,
                        'ion.services.mi.sbe37_driver', 'SBE37Driver')
        driver_client = ZmqDriverClient('localhost', 5556, 5557)
        driver_client.start_messaging()
        time.sleep(3)
        
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
        driver_client.done()
        """
        pass
    
    
    def test_number_2(self):
        """
        """
        
        pass 