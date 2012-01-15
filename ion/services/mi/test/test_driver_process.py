#!/usr/bin/env python

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import time
import unittest
from mock import Mock, sentinel, patch
from nose.plugins.attrib import attr
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.core.exception import NotFound

import ion.services.mi.driver_process as dp
import ion.services.mi.driver_client as dc
from pyon.public import IonObject, log

#class DriverProcessTest(PyonTestCase):
#class DriverProcessTest(IonIntegrationTestCase):

# Make tests verbose and provide stdout
#bin/nosetests -s -v ion/services/mi/test/test_driver_process.py

@attr('UNIT', group='mi')
class DriverProcessTest(PyonTestCase):
    

    def setUp(self):
        """
        """
        # Zmq parameters used by driver process and client.
        self.host = 'localhost'
        self.cmd_port = 5556
        self.event_port = 5557
        
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
            
        log.info('\n')
        # Create driver process.
        log.info('Creating driver process.')
        self.driver_process = dp.ZmqDriverProcess(self.cmd_port, self.host,
            self.event_port, self.driver_module, self.driver_class)
        self.assertIsInstance(self.driver_process, dp.DriverProcess)
        log.info('Driver process created.')
    
        log.info('Creating client object.')
        self.driver_client = dc.ZmqDriverClient(self.host, self.cmd_port,
                                                self.event_port)
        self.assertIsInstance(self.driver_client, dc.DriverClient)
        log.info('Client created.')

        # Start the driver process.
        log.info('Starting driver process.')
        self.driver_process.start()
        log.info('Driver process started.')
        
        # Start the client messaging.
        log.info('Starting client messaging.')
        self.driver_client.start_messaging()
        log.info('Client messaging started.')
        
        log.info('Sending test message.')
        reply = self.driver_client.cmd_driver('zoom zoom')
        log.info('Got reply: %s' % reply)
        
        log.info('Sending stop message.')
        reply = self.driver_client.cmd_driver('stop_driver_process')
        log.info('Got reply: %s' % reply)
        
        log.info('Shutting down client comms.')
        self.driver_client.stop_messaging()
        log.info('Done shutting down client comms.')
        
        self.driver_client = None
        self.driver_process = None
        
    def test_number_2(self):
        """
        """
        
        print 'in test 2'
        