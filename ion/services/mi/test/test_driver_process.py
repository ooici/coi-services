#!/usr/bin/env python

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import unittest
from mock import Mock, sentinel, patch
from nose.plugins.attrib import attr
from pyon.util.unit_test import PyonTestCase
from pyon.core.exception import NotFound

from ion.services.mi.driver_process import DriverProcess
from ion.services.mi.driver_process import DriverClient

@attr('UNIT', group='sa')
class DriverProcessTest(PyonTestCase):

    def setUp(self):
        """
        """
        pass
    
    def test_driver_process(self):
        """
        """
        dp = DriverProcess(5562)
        self.assertIsInstance(dp, DriverProcess)
        
