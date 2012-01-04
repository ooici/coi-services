#!/usr/bin/env python

'''
@file ion/services/sa/instrument/test/test_int_data_acquisition_management_service.py
@author Maurice Manning
@test ion.services.sa.acquisition.DataAcquisitionManagementService integration test
'''

from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
import unittest

@attr('INT', group='sa')
class TestIntDataAcquisitionManagementService(IonIntegrationTestCase):

    def setUp(self):
        pass

    @unittest.skip('Not done yet.')
    def test_register_producer_and_send(self):
        # Register an instrument in coordination with DM PubSub: create stream, register and create producer object
        pass

    @unittest.skip('Not done yet.')
    def test_register_process_and_send(self):
        # Register a transform in coordination with DM PubSub: create stream, register and create producer object
        pass
