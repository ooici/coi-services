#!/usr/bin/env python

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'



from mock import Mock, sentinel, patch
from pyon.util.unit_test import PyonTestCase
#from ion.services.dm.distribution.pubsub_management_service import PubsubManagementService
from nose.plugins.attrib import attr
from pyon.core.exception import NotFound
import unittest

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
        #pass
        self.assertEqual(4, 2+2)   