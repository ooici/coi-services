#!/usr/bin/env python

"""
@file  ion.services.sa.process.test.
          test_data_process_management_service
@author   Alon Yaari
@test ion.services.sa.process.data_process_management_service
          Unit test suite to cover all service code
"""

from nose.plugins.attrib import attr
from pyon.util.unit_test import pop_last_call, PyonTestCase
from pyon.public import IonObject, RT, AT
from ion.services.sa.process.data_process_management_service \
import DataProcessManagementService
import unittest
import time

mockDataProcessObj = \
    'ion.services.sa.process.data_process_management_service.IonObject'
    
@attr('UNIT', group='sa')
class Test_DataProcessManagementService_Unit(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock(mockDataProcessObj)
        self.mock_clients = self._create_service_mock('data_process_management')
        self.data_process_mgmt_service = DataProcessManagementService()
        self.data_process_mgmt_service.clients = self.mock_clients

        # Create a data process definition
        self.data_process_def_name = "TestDataProcessDef_X"
        self.data_process_def_obj = IonObject(RT.DataProcessDefinition,
                                      name=self.data_process_def_name)

        # Define some variables for use in the tests
        self.in_product_A = "ID4INprodA"
        self.in_product_B = "ID4INprodB"
        self.out_product_A = "ID4OUTprodA"
        self.out_product_B = "ID4OUTprodB"
        self.data_proc_def_id = "ID4procDef"

        self.data_process_name = "process_" + self.data_process_def_name
        self.data_process_object = IonObject(RT.DataProcess, name=self.data_process_name)
        self.data_process_id = "ID4process"
        
        self.transform_name = self.data_process_name + " - calculates " + \
                              self.out_product_A + time.ctime()
        self.transform_object = IonObject(RT.Transform, name=self.transform_name)
        self.transform_id = "ID4transform"

    def test_create_data_process(self):
        # setup
        self.mock_ionobj.return_value = self.transform_object
        self.resource_registry.read.return_value = (self.data_process_def_obj)
        self.resource_registry.create.return_value = (self.data_process_id, 'Version_1')
        self.transform_management_service.create_transform.return_value = self.transform_id
        # these are listed in reverse of chronological order 
        results = [self.transform_object,
                   self.data_process_object]
        def side_effect(*args, **kwargs):
            return results.pop()
        self.mock_ionobj.side_effect = side_effect
         
        # test call
        dp_id = self.data_process_mgmt_service.create_data_process \
                    (self.data_proc_def_id, \
                     self.in_product_A, \
                     self.out_product_A)

        # verify results
        self.assertEqual(dp_id, 'ID4process')
        self.resource_registry.read.assert_called_once_with(self.data_proc_def_id, '')
        self.assertEqual(self.mock_ionobj.call_count, 2)
        self.mock_ionobj.assert_called_with(RT.Transform, name=self.transform_name)
        pop_last_call(self.mock_ionobj)
        self.mock_ionobj.assert_called_once_with(RT.DataProcess, name=self.data_process_name)
        self.resource_registry.create.assert_called_once_with(self.data_process_object)
        self.transform_management_service.create_transform.assert_called_once_with(self.transform_object)
        self.transform_management_service.schedule_transform.assert_called_once_with(self.transform_id)
        self.transform_management_service.bind_transform.assert_called_once_with(self.transform_id)
        self.assertEqual(self.resource_registry.create_association.call_count, 3)
        self.resource_registry.create_association.assert_called_with(self.data_process_id, 
                                                                     AT.hasTransform,
                                                                     self.transform_id,
                                                                     None)
        pop_last_call(self.resource_registry.create_association)
        self.resource_registry.create_association.assert_called_with(self.data_process_id, 
                                                                     AT.hasOutputProduct,
                                                                     self.out_product_A,
                                                                     None)
        pop_last_call(self.resource_registry.create_association)
        self.resource_registry.create_association.assert_called_once_with(self.data_process_id, 
                                                                          AT.hasInputProduct,
                                                                          self.in_product_A,
                                                                          None)

    @unittest.skip('all operations not working yet for unit tests to pass')
    def test_update_data_process(self):
        # Create a new data process
        data_proc_name = "TestDataProcessDefIDstring"
        dp_id = self.data_process_mgmt_service.create_data_process\
            (self.data_proc_def_id,\
             self.in_product_A,\
             self.out_product_A)
        # Update the data process with a new in and out

        
