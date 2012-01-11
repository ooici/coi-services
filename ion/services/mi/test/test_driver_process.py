#!/usr/bin/env python

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import unittest
from mock import Mock, sentinel, patch
from nose.plugins.attrib import attr
from pyon.util.unit_test import PyonTestCase
from pyon.core.exception import NotFound

import ion.services.mi.driver_process as dp
import ion.services.mi.driver_client as dc

@attr('UNIT', group='sa')
class DriverProcessTest(PyonTestCase):

    def setUp(self):
        """
        """
        
        # Create the driver process.
        self.driver_process = dp.ZmqDriverProcess(5556, 'localhost', 5557,
                              'ion.services.mi.sbe37_driver', 'SBE37Driver')
        self.assertIsInstance(self.driver_process, dp.DriverProcess)

        # Creatae the driver client.        
        self.driver_client = dc.ZmqDriverClient('localhost', 5556, 5557)
        self.assertIsInstance(self.driver_client, dc.DriverClient)
        
        """
        # Start the driver process.
        self.driver_process.start()
        
        # Start the client messaging.
        self.driver_client.start_messaging()
        """
        
    def test_driver_process(self):
        """
        """
        #dp = DriverProcess(5562)
        #self.assertIsInstance(dp, DriverProcess)
        pass


"""
process = dp.ZmqDriverProcess(5556, 'localhost', 5557,
                              'ion.services.mi.sbe37_driver', 'SBE37Driver')

process.start()
"""
    