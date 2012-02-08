'''
@author Luke Campbell <luke.s.campbell@gmail.com>
@file ion/services/dm/inventory/test/dataset_management_test.py
@description Unit and Integration test implementations for the data set management service class.
'''
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from interface.objects import DataSet
from pyon.util.containers import DotDict
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr


@attr('UNIT',group='DM')
class DatasetManagementTest(PyonTestCase):
    def setUp(self):
        mock_clients = self._create_service_mock('dataset_management')
        self.dataset_management = DatasetManagementService()
        self.dataset_management.clients = mock_clients

        self.mock_rr_create = self.dataset_management.clients.resource_registry.create
        self.mock_rr_read = self.dataset_management.clients.resource_registry.read
        self.mock_rr_update = self.dataset_management.clients.resource_registry.update
        self.mock_rr_delete = self.dataset_management.clients.resource_registry.delete

    def test_create_dataset(self):
        # mocks
        self.mock_rr_create.return_value = ('dataset_id','rev')
        mock_dataset = {'dataset':'mocked'}

        # execution
        dataset_id = self.dataset_management.create_dataset(dataset=mock_dataset)


        # assertions
        self.assertEquals(dataset_id,'dataset_id')
        self.mock_rr_create.assert_called_with(mock_dataset)

    def test_update_dataset(self):
        # mocks
        mock_dataset = DotDict({'_id':'dataset_id'})


        # execution
        self.dataset_management.update_dataset(mock_dataset)


        # assertions
        self.mock_rr_update.assert_called_with(mock_dataset)

    def test_delete_dataset(self):
        # mocks

        # execution
        self.dataset_management.delete_dataset('123')

        # assertions
        self.mock_rr_delete.assert_called_with('123')