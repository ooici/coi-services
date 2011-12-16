#!/usr/bin/env python

'''
@file ion/services/sa/instrument_management/test/test_int_data_acquisition_management_service.py
@author Maurice Manning
@test ion.services.sa.data_acquisition_management.DataAcquisitionManagementService integration test
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
        # One liner describing the test
        pass

    @unittest.skip('Not done yet.')
    def test_register_process_and_send(self):
        # One liner describing the test
        pass
