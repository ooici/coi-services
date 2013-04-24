#!/usr/bin/env python

'''
@file ion/services/sa/acquisition/test/test_data_acquisition_management_service.py
@author Maurice Manning
@test ion.services.sa.acquisition.data_acquisition_management_service Unit test suite to cover all service code
'''
from ion.services.sa.test.helpers import UnitTestGenerator

from mock import Mock, sentinel, patch
from pyon.util.unit_test import PyonTestCase
from ion.services.sa.acquisition.data_acquisition_management_service import DataAcquisitionManagementService
from nose.plugins.attrib import attr
from pyon.public import RT, PRED

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
import unittest

@attr('UNIT', group='sa')
class TestDataAcquisitionManagement(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.sa.acquisition.data_acquisition_management_service.IonObject')
        mock_clients = self._create_service_mock('data_acquisition_management')

        self.data_acquisition_mgmt_service = DataAcquisitionManagementService()
        self.data_acquisition_mgmt_service.clients = mock_clients

        # must call this manually
        self.data_acquisition_mgmt_service.on_init()

        # save some typing
        self.mock_create = mock_clients.resource_registry.create
        self.mock_update = mock_clients.resource_registry.update
        self.mock_delete = mock_clients.resource_registry.delete
        self.mock_read = mock_clients.resource_registry.read
        self.mock_create_association = mock_clients.resource_registry.create_association

        self.data_source = Mock()
        self.data_source.name = 'dsname'
        self.data_source.type = 'foo'
        self.data_source.description = 'data source desc'
        self.data_source.connection_params = {'param1':'111'}

        self.data_producer = Mock()
        self.data_producer.name = 'dproducer'
        self.data_producer.type = 'instrument'


        self.data_process = Mock()
        self.data_process.name = 'dprocess'
        self.data_process.description = 'process desc'

        self.instrument = Mock()
        self.instrument.name = 'inst'
        self.instrument.description = 'inst desc'




#    def test_register_process(self):
#        self.mock_read.return_value = self.data_process
#        self.mock_create.return_value = ('111', 'bla')
#        self.mock_create.return_value = ('222', 'bla')
#        self.mock_create_association.return_value = ['333', 1]
#
#        self.data_acquisition_mgmt_service.register_process('111')

        #self.mock_read.assert_called_once_with('111', '')
        #self.mock_create_association.assert_called_once_with('111', PRED.hasDataProducer, '222', None)

    def test_register_process_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.data_acquisition_mgmt_service.register_process('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Data Process bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

#    def test_register_data_set(self):
#        self.mock_read.return_value = self.data_process
#        self.mock_create.return_value = ('111', 'bla')
#        self.mock_create.return_value = ('222', 'bla')
#        self.mock_create_association.return_value = ['333', 1]
#
#        self.data_acquisition_mgmt_service.register_external_data_set('111')

        #self.mock_read.assert_called_once_with('111', '')
        #self.mock_create_association.assert_called_once_with('111', PRED.hasDataProducer, '222', None)

    def test_register_data_source_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.data_acquisition_mgmt_service.register_external_data_set('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'External Data Set bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

#    def test_register_instrument(self):
#        self.mock_read.return_value = self.instrument
#        self.mock_create.return_value = ('111', 'bla')
#        self.mock_create.return_value = ('222', 'bla')
#        self.mock_create_association.return_value = ['333', 1]
#
#        self.data_acquisition_mgmt_service.register_instrument('111')



utg = UnitTestGenerator(TestDataAcquisitionManagement,
                        DataAcquisitionManagementService)

utg.test_all_in_one(True)

utg.add_resource_unittests(RT.ExternalDataProvider, "external_data_provider", {})
utg.add_resource_unittests(RT.DataSource, "data_source", {})
utg.add_resource_unittests(RT.DataSourceModel, "data_source_model", {})
utg.add_resource_unittests(RT.DataSourceAgent, "data_source_agent", {})
utg.add_resource_unittests(RT.DataSourceAgentInstance, "data_source_agent_instance", {})
utg.add_resource_unittests(RT.ExternalDataset, "external_dataset", {})
utg.add_resource_unittests(RT.ExternalDatasetAgent, "external_dataset_agent", {})
utg.add_resource_unittests(RT.ExternalDatasetAgentInstance, "external_dataset_agent_instance", {})
