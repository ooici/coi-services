'''
@author Luke Campbell <luke.s.campbell@gmail.com>
@file ion/services/dm/inventory/test/dataset_management_test.py
@description Unit and Integration test implementations for the data set management service class.
'''
import unittest
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from prototype.sci_data.stream_defs import ctd_stream_packet
from pyon.datastore.datastore import DataStore
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from ion.services.dm.utility.granule_utils import CoverageCraft, SimplexCoverage
from nose.plugins.attrib import attr
from mock import Mock, patch
import random
import unittest


@attr('UNIT',group='dm')
class DatasetManagementTest(PyonTestCase):
    def setUp(self):
        mock_clients = self._create_service_mock('dataset_management')
        self.dataset_management = DatasetManagementService()
        self.dataset_management.clients = mock_clients

        self.mock_rr_create = self.dataset_management.clients.resource_registry.create
        self.mock_rr_read = self.dataset_management.clients.resource_registry.read
        self.mock_rr_update = self.dataset_management.clients.resource_registry.update
        self.mock_rr_delete = self.dataset_management.clients.resource_registry.delete
        self.mock_rr_create_assoc = self.dataset_management.clients.resource_registry.create_association
        self.mock_rr_find_assocs = self.dataset_management.clients.resource_registry.find_associations
        self.mock_rr_delete_assoc = self.dataset_management.clients.resource_registry.delete_association

    def test_create_dataset(self):
        # mocks
        self.mock_rr_create.return_value = ('dataset_id','rev')

        # execution
        self.dataset_management._create_coverage = Mock()
        self.dataset_management._persist_coverage = Mock()
        dataset_id = self.dataset_management.create_dataset(name='123',stream_id='123',datastore_name='fake_datastore', parameter_dict=[0], spatial_domain=[0], temporal_domain=[0])


        # assertions
        self.assertEquals(dataset_id,'dataset_id')
        self.assertTrue(self.mock_rr_create.called)
        self.assertTrue(self.mock_rr_create_assoc.call_count)


    def test_create_coverage(self):
        craft = CoverageCraft
        sdom, tdom = craft.create_domains()
        sdom = sdom.dump()
        tdom = tdom.dump()
        pdict = craft.create_parameters()
        pdict = pdict.dump()

        coverage = self.dataset_management._create_coverage("doesn't matter", pdict, sdom, tdom)
        self.assertIsInstance(coverage,SimplexCoverage)

    @patch('ion.services.dm.inventory.dataset_management_service.SimplexCoverage')
    @patch('ion.services.dm.inventory.dataset_management_service.validate_is_instance')
    def test_persist_coverage(self,validation, cov_mock):
        validation = Mock()
        cov_mock.save = Mock()
        mock_bb = CoverageCraft()
        self.dataset_management._persist_coverage('dataset_id', mock_bb.coverage)


    @patch('ion.services.dm.inventory.dataset_management_service.SimplexCoverage')
    def test_get_coverage(self, cov_mock):
        cov_mock.load = Mock()
        cov_mock.load.return_value = 'test'

        retval = self.dataset_management._get_coverage('dataset_id')
        self.assertEquals(retval,'test')


    def test_update_dataset(self):
        # mocks
        mock_dataset = DotDict({'_id':'dataset_id'})


        # execution
        self.dataset_management.update_dataset(mock_dataset)


        # assertions
        self.mock_rr_update.assert_called_with(mock_dataset)

    def test_delete_dataset(self):
        # mocks
        self.mock_rr_find_assocs.return_value = ['assoc']

        # execution
        self.dataset_management.delete_dataset('123')

        # assertions
        self.mock_rr_delete.assert_called_with('123')
        self.assertTrue(self.mock_rr_delete_assoc.call_count == 1)

    def test_add_stream(self):
        self.dataset_management.add_stream('dataset_id','stream_id')
        self.assertTrue(self.mock_rr_create_assoc.call_count)
    
    def test_remove_stream(self):
        self.mock_rr_find_assocs.return_value = [0]
        self.dataset_management.remove_stream('dataset_id','stream_id')
        self.assertTrue(self.mock_rr_delete_assoc.call_count)


@attr('INT', group='dm')
class DatasetManagementIntTest(IonIntegrationTestCase):
    pass
