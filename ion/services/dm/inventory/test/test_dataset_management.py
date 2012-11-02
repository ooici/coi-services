'''
@author Luke Campbell <luke.s.campbell@gmail.com>
@file ion/services/dm/inventory/test/dataset_management_test.py
@description Unit and Integration test implementations for the data set management service class.
'''
from pyon.core.exception import NotFound
from pyon.util.int_test import IonIntegrationTestCase

from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule_utils import time_series_domain

from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from coverage_model.parameter import QuantityType, ParameterContext, ParameterDictionary

from nose.plugins.attrib import attr

import numpy as np


@attr('INT', group='dm')
class DatasetManagementIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.resource_registry  = ResourceRegistryServiceClient()
        self.dataset_management = DatasetManagementServiceClient()

    def test_dataset_crud(self):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        tdom, sdom = time_series_domain()
        dataset_id = self.dataset_management.create_dataset(name='ctd_dataset', parameter_dictionary_id=pdict_id, spatial_domain=sdom.dump(), temporal_domain=tdom.dump())

        ds_obj = self.dataset_management.read_dataset(dataset_id)
        self.assertEquals(ds_obj.name, 'ctd_dataset')
        
        ds_obj.name = 'something different'
        self.dataset_management.update_dataset(ds_obj)
        self.dataset_management.register_dataset(dataset_id)
        ds_obj2 = self.dataset_management.read_dataset(dataset_id)
        self.assertEquals(ds_obj.name, ds_obj2.name)
        self.assertTrue(ds_obj2.registered)

        

   
    
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
        pdict_res_id = self.dataset_management.create_parameter_dictionary(name='pdict1', parameter_context_ids=context_ids, temporal_context='time')

        pdict_contexts = self.dataset_management.read_parameter_contexts(parameter_dictionary_id=pdict_res_id, id_only=True)

        pdict = DatasetManagementService.get_parameter_dictionary(pdict_res_id)
        self.assertIsInstance(pdict, ParameterDictionary)
        self.assertTrue('time' in pdict)
        self.assertEquals(pdict.identifier, pdict_res_id)

        self.assertEquals(set(pdict_contexts), set(context_ids))

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
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0
        context_ids.append(self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump()))

        return context_ids

