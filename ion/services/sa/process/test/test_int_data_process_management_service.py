#!/usr/bin/env python

'''
@file ion/services/sa/process/test/test_int_data_process_management_service.py
@author Alon Yaari
@test ion.services.sa.process.DataProcessManagementService integration test
'''

from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
import unittest

@attr('INT', group='sa')
class TestIntDataProcessManagementService(IonIntegrationTestCase):

    def setUp(self):
        pass

    @unittest.skip('Not done yet.')
    def test_create_update_delete_data_process(self):
        # Test the creation of a calibration data process- it brings level0 data to level1
        #   Prerequisites
        #       - A level0 data product (raw parsed data coming out of an instrument) must
        #               be streaming into an existing data product
        #   create() Test:
        #       - Test will create a new data product
        #       - Call create_data_process(), passing in the level0 data product, the new
        #               data product, and a data transform definition
        #   create() Identification of Success:
        #       - Data process stored in the resource registry
        #       - Transform stored in the resource registry
        #       - Data stream published by the new data product is a match for data being
        #           published into the level0 data product (plus the calibration transform)
        #   update() Test:
        #       - Test creates yet another data product
        #       - Call update_data_process(), passing in the new data product to publish to
        #   update() Identification of Success:
        #       - Data process association changed from original data product to new one
        #       - Original data product has no active data stream
        #       - The new data product has active data stream matching the level0 product
        #   delete() Test:
        #       - Test deletes the data process
        #   delete() Identification of Success:
        #       - Data product continues to exist but no longer publishes
        #       - Child transform is no longer in the resource registry
        #       - Data process is no longer in the resource registry
        #       - Association to the data process no longer exist
        pass
