#!/usr/bin/env python

"""
@file  ion.services.sa.process.test.
          test_data_process_management_service
@author   Alon Yaari
@test ion.services.sa.process.data_process_management_service
          Unit test suite to cover all service code
"""
from ion.services.sa.test.helpers import UnitTestGenerator

from nose.plugins.attrib import attr
from pyon.util.unit_test import pop_last_call, PyonTestCase
from pyon.public import IonObject, RT, PRED
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
        self.data_process_mgmt_service.on_init()

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
        self.data_prod_id = "ID4producer"
        self.subscription_id = "ID4subscription"
        self.stream_id = "ID4stream"

        self.data_process_name = "process_" + self.data_process_def_name \
                                 + " - calculates " + \
                                 self.out_product_A + time.ctime()
        self.data_process_object = IonObject(RT.DataProcess, name=self.data_process_name)
        self.data_process_id = "ID4process"
        
        self.transform_id = "ID4transform"

    @unittest.skip('not working')
    def test_create_data_process(self):
        # setup
        self.resource_registry.read.return_value = (self.data_process_def_obj)
        self.resource_registry.create.return_value = (self.data_process_id, 'Version_1')
        self.transform_management_service.create_transform.return_value = self.transform_id
        self.data_acquisition_management.register_process.return_value = self.data_prod_id
        self.mock_ionobj.return_value = self.data_process_object
         
        # test call
        dp_id = self.data_process_mgmt_service.create_data_process \
                    (self.data_proc_def_id, \
                     [self.in_product_A], \
                     self.out_product_A)

        # verify results
        self.assertEqual(dp_id, 'ID4process')
        self.resource_registry.read.assert_called_once_with(self.data_proc_def_id, '')
        self.mock_ionobj.assert_called_once_with(RT.DataProcess, name=self.data_process_name)
        self.resource_registry.create.assert_called_once_with(self.data_process_object)
        self.data_acquisition_management.register_process.assert_called_once_with(self.data_process_id)
        self.transform_management_service.create_transform.assert_called_once_with(self.data_prod_id,
                                                                                   self.subscription_id,
                                                                                   self.stream_id)
        self.transform_management_service.schedule_transform.assert_called_once_with(self.transform_id)
        self.transform_management_service.bind_transform.assert_called_once_with(self.transform_id)
        self.assertEqual(self.resource_registry.create_association.call_count, 3)
        self.resource_registry.create_association.assert_called_with(self.data_process_id, 
                                                                     PRED.hasTransform,
                                                                     self.transform_id,
                                                                     None)
        pop_last_call(self.resource_registry.create_association)
        self.resource_registry.create_association.assert_called_with(self.data_process_id, 
                                                                     PRED.hasOutputProduct,
                                                                     self.out_product_A,
                                                                     None)
        pop_last_call(self.resource_registry.create_association)
        self.resource_registry.create_association.assert_called_once_with(self.data_process_id, 
                                                                          PRED.hasInputProduct,
                                                                          self.in_product_A,
                                                                          None)
    @unittest.skip('not working')
    def test_read_data_process(self):
        # setup
        self.resource_registry.find_associations.return_value = ([self.transform_id], "don't care")
        self.transform_object.data_process_definition_id = self.data_proc_def_id
        self.transform_object.in_subscription_id = self.in_product_A
        self.transform_object.out_data_product_id = self.out_product_A
        self.transform_management_service.read_transform.return_value = self.transform_object
         
        # test call
        dpd_id, in_id, out_id = self.data_process_mgmt_service.read_data_process(self.data_process_id)

        # verify results
        self.assertEqual(dpd_id, self.data_proc_def_id)
        self.assertEqual(in_id, self.in_product_A)
        self.assertEqual(out_id, self.out_product_A)
        self.resource_registry.find_associations.assert_called_once_with(self.data_process_id, PRED.hasTransform, '', None, False)
        self.transform_management_service.read_transform.assert_called_once_with(self.transform_id)



utg = UnitTestGenerator(Test_DataProcessManagementService_Unit,
                        DataProcessManagementService)

utg.test_all_in_one(True)

utg.add_resource_unittests(RT.TransformFunction, "transform_function", {})
utg.add_resource_unittests(RT.DataProcessDefinition, "data_process_definition", {})
#utg.add_resource_unittests(RT.DataProcess, "data_process", {})
#utg.add_resource_unittests(RT.DataProcess, "data_process2", {})

