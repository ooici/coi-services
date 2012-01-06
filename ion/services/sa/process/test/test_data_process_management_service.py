#!/usr/bin/env python

"""
@file  ion.services.sa.process.test.
          test_data_process_management_service
@author   Alon Yaari
@test ion.services.sa.process.data_process_management_service
          Unit test suite to cover all service code
"""

from nose.plugins.attrib import attr
from pyon.util.unit_test import PyonTestCase
from pyon.public import IonObject, RT
from ion.services.sa.process.data_process_management_service \
import DataProcessManagementService
import unittest

mockDataProcessObj = 'ion.services.sa.process.' + \
                     'data_process_management_service.IonObject'
@attr('UNIT', group='sa')
@unittest.skip('all operations not working yet for unit tests to pass')
class Test_DataProcessManagementService_Unit(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock(mockDataProcessObj)
        self.mock_clients = self._create_service_mock('data_process_management')
        self.data_process_mgmt_service = DataProcessManagementService()
        self.data_process_mgmt_service.clients = self.mock_clients

        # Create a data definition
        data_proc_def_name = "TestDataProcessDefIDstring"
        data_proc_def_obj = IonObject(RT.DataProcessDefinition,
                                      name=data_proc_def_name)
        def_id = self.data_process_mgmt_service.create_data_process_definition\
                    (data_proc_def_obj)

        # Define some variables for use in the tests
        self.in_product_A = "ID4INprodA"
        self.in_product_B = "ID4INprodB"
        self.out_product_A = "ID4OUTprodA"
        self.out_product_B = "ID4OUTprodB"
        self.data_proc_def_id = def_id

    def test_create_data_process(self):
        """
        """
        data_proc_name = "TestDataProcessDefIDstring"
        dp_id = self.data_process_mgmt_service.create_data_process \
                    (self.data_proc_def_id, \
                     self.in_product_A, \
                     self.out_product_A)

        # Verify
        self.assertIsNotNone(dp_id)
        dp_obj = self.res

    def test_update_data_process(self):
        """
        """
        # Create a new data process
        data_proc_name = "TestDataProcessDefIDstring"
        dp_id = self.data_process_mgmt_service.create_data_process\
            (self.data_proc_def_id,\
             self.in_product_A,\
             self.out_product_A)
        # Update the data process with a new in and out
        
