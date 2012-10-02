'''
@author Luke Campbell <luke.s.campbell@gmail.com>
@file ion/services/dm/inventory/test/dataset_management_test.py
@description Unit and Integration test implementations for the data set management service class.
'''
from pyon.core.exception import NotFound
from pyon.datastore.datastore import DataStore
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase

from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule_utils import CoverageCraft, SimplexCoverage

from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from coverage_model.parameter import QuantityType, ParameterContext, ParameterDictionary, AxisTypeEnum

from nose.plugins.attrib import attr
from mock import Mock, patch


import numpy as np
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

    def test_get_dataset_info(self):
        coverage = DotDict()
        coverage.info = 1

        self.dataset_management._get_coverage = Mock()
        self.dataset_management._get_coverage.return_value = coverage

        retval = self.dataset_management.get_dataset_info('dataset_id')
        self.assertEquals(retval,1)


    def test_get_dataset_parameters(self):
        coverage = DotDict()
        coverage.parameter_dictionary.dump = Mock()
        coverage.parameter_dictionary.dump.return_value = 1

        self.dataset_management._get_coverage = Mock()
        self.dataset_management._get_coverage.return_value = coverage

        retval = self.dataset_management.get_dataset_parameters('dataset_id')
        self.assertEquals(retval,1)


@attr('INT', group='dm')
class DatasetManagementIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.resource_registry = ResourceRegistryServiceClient()
        self.dataset_management = DatasetManagementServiceClient()
    
    def test_context_crud(self):
        context_ids = self.create_contexts()
        context_id = context_ids.pop()

        context = DatasetManagementService.get_parameter_context(context_id)
        self.assertIsInstance(context, ParameterContext)
        self.assertEquals(context.identifier, context_id)

        self.dataset_management.delete_parameter_context(context_id)

        with self.assertRaises(NotFound):
            self.dataset_management.read_parameter_context(context_id)

    def test_pdict_crud(self):
        context_ids = self.create_contexts()
        pdict_res_id = self.dataset_management.create_parameter_dictionary(name='pdict1', parameter_context_ids=context_ids)

        pdict_contexts = self.dataset_management.read_parameter_contexts(parameter_dictionary_id=pdict_res_id, id_only=True)

        pdict = DatasetManagementService.get_parameter_dictionary(pdict_res_id)
        self.assertIsInstance(pdict, ParameterDictionary)
        self.assertTrue('time' in pdict)
        self.assertEquals(pdict.identifier, pdict_res_id)

        self.assertEquals(set(pdict_contexts), set(context_ids))

        context_id = context_ids.pop()
        self.dataset_management.remove_context(parameter_dictionary_id=pdict_res_id, parameter_context_ids=[context_id])

        self.assertTrue(context_id not in self.dataset_management.read_parameter_contexts(parameter_dictionary_id=pdict_res_id, id_only=True))

        self.dataset_management.add_context(parameter_dictionary_id=pdict_res_id, parameter_context_ids=[context_id])

        self.assertTrue(context_id in self.dataset_management.read_parameter_contexts(parameter_dictionary_id=pdict_res_id, id_only=True))

        self.dataset_management.delete_parameter_dictionary(parameter_dictionary_id=pdict_res_id)
        with self.assertRaises(NotFound):
            self.dataset_management.read_parameter_dictionary(parameter_dictionary_id=pdict_res_id)

    def create_contexts(self):
        context_ids = []
        cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=np.float32))
        cond_ctxt.uom = 'unknown'
        cond_ctxt.fill_value = 0e0
        context_ids.append(self.dataset_management.create_parameter_context(name='conductivity', parameter_context=cond_ctxt.dump()))

        pres_ctxt = ParameterContext('pressure', param_type=QuantityType(value_encoding=np.float32))
        pres_ctxt.uom = 'Pascal'
        pres_ctxt.fill_value = 0x0
        context_ids.append(self.dataset_management.create_parameter_context(name='pressure', parameter_context=pres_ctxt.dump()))

        sal_ctxt = ParameterContext('salinity', param_type=QuantityType(value_encoding=np.float32))
        sal_ctxt.uom = 'PSU'
        sal_ctxt.fill_value = 0x0
        context_ids.append(self.dataset_management.create_parameter_context(name='salinity', parameter_context=sal_ctxt.dump()))

        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.float32))
        temp_ctxt.uom = 'degree_Celsius'
        temp_ctxt.fill_value = 0e0
        context_ids.append(self.dataset_management.create_parameter_context(name='temp', parameter_context=temp_ctxt.dump()))

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.int64))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0
        context_ids.append(self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump()))

        return context_ids
