#!/usr/bin/env python

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

#import pyon.core.exception as pe

import time
import unittest
import logging

from nose.plugins.attrib import attr
#from mock import Mock, sentinel, patch

#from pyon.util.unit_test import PyonTestCase
#from pyon.util.int_test import IonIntegrationTestCase
#from pyon.core.exception import NotFound
#from pyon.public import IonObject, log
import ion.services.mi.mi_logger
import ion.services.mi.driver_process as dp
import ion.services.mi.driver_client as dc

mi_logger = logging.getLogger('mi_logger')

# Make tests verbose and provide stdout
# bin/nosetests -s -v ion/services/mi/test/test_driver_process.py

@attr('UNIT', group='mi')
class DriverProcessTest(unittest.TestCase):    

    def setUp(self):
        """
        """
        # Zmq parameters used by driver process and client.
        self.host = 'localhost'
        self.cmd_port = 5556
        self.evt_port = 5557
        
        # Driver module parameters.
        self.driver_module = 'ion.services.mi.sbe37_driver'
        self.driver_class = 'SBE37Driver'


        """
        print 'running the setup function\n'
        def print_cleanup():
            print 'in the cleanup function\n'
        self.addCleanup(print_cleanup)
        """
        
    def test_driver_process(self):
        """
        """
        """
        driver_process = dp.ZmqDriverProcess(5556, 5557,
                            'ion.services.mi.sbe37_driver', 'SBE37Driver')
        driver_client = dc.ZmqDriverClient('localhost', 5556, 5557)
        driver_process.start()
        driver_client.start_messaging()
        time.sleep(1)

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

        reply = driver_client.cmd_dvr('test_events')
        self.assertEqual(reply, 'test_events')
        time.sleep(1)
        self.assertTrue(driver_client.events, ['I am event number 1!',
                                               'And I am event number 2!'])
        driver_client.done()
        """
        pass
    
    
    def test_number_2(self):
        """
        """
        
        pass 