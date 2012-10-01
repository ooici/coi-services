#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Mon Oct  1 13:26:07 EDT 2012
@file ion/services/dm/distribution/test/test_parameter_management_service.py
@description Service Implementation for Parameter Management Service Test
'''

from nose.plugins.attrib import attr
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iparameter_management_service import ParameterManagementServiceClient
from coverage_model.parameter import QuantityType, ParameterContext, ParameterDictionary, AxisTypeEnum
from ion.services.dm.distribution.parameter_management_service import ParameterManagementService
from pyon.core.exception import NotFound

import numpy as np

@attr('UNIT',group='dm')
class ParameterManagementUnitTest(PyonTestCase):
    pass

@attr('INT', group='dm')
class ParameterManagementIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.resource_registry = ResourceRegistryServiceClient()
        self.parameter_management = ParameterManagementServiceClient()
    
    def test_context_crud(self):
        context_ids = self.create_contexts()
        context_id = context_ids.pop()

        context = ParameterManagementService.get_parameter_context(context_id)
        self.assertIsInstance(context, ParameterContext)
        self.assertEquals(context.identifier, context_id)

        self.parameter_management.delete_parameter_context(context_id)

        with self.assertRaises(NotFound):
            self.parameter_management.read_parameter_context(context_id)

    def test_pdict_crud(self):
        context_ids = self.create_contexts()
        pdict_res_id = self.parameter_management.create_parameter_dictionary(name='pdict1', parameter_context_ids=context_ids)

        pdict_contexts = self.parameter_management.read_parameter_contexts(parameter_dictionary_id=pdict_res_id, id_only=True)

        pdict = ParameterManagementService.get_parameter_dictionary(pdict_res_id)
        self.assertIsInstance(pdict, ParameterDictionary)
        self.assertTrue('time' in pdict)
        self.assertEquals(pdict.identifier, pdict_res_id)

        self.assertEquals(set(pdict_contexts), set(context_ids))

        context_id = context_ids.pop()
        self.parameter_management.remove_context(parameter_dictionary_id=pdict_res_id, parameter_context_ids=[context_id])

        self.assertTrue(context_id not in self.parameter_management.read_parameter_contexts(parameter_dictionary_id=pdict_res_id, id_only=True))

        self.parameter_management.add_context(parameter_dictionary_id=pdict_res_id, parameter_context_ids=[context_id])

        self.assertTrue(context_id in self.parameter_management.read_parameter_contexts(parameter_dictionary_id=pdict_res_id, id_only=True))

        self.parameter_management.delete_parameter_dictionary(parameter_dictionary_id=pdict_res_id)
        with self.assertRaises(NotFound):
            self.parameter_management.read_parameter_dictionary(parameter_dictionary_id=pdict_res_id)


    def create_contexts(self):
        context_ids = []
        cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=np.float32))
        cond_ctxt.uom = 'unknown'
        cond_ctxt.fill_value = 0e0
        context_ids.append(self.parameter_management.create_parameter_context(name='conductivity', parameter_context=cond_ctxt.dump()))

        pres_ctxt = ParameterContext('pressure', param_type=QuantityType(value_encoding=np.float32))
        pres_ctxt.uom = 'Pascal'
        pres_ctxt.fill_value = 0x0
        context_ids.append(self.parameter_management.create_parameter_context(name='pressure', parameter_context=pres_ctxt.dump()))

        sal_ctxt = ParameterContext('salinity', param_type=QuantityType(value_encoding=np.float32))
        sal_ctxt.uom = 'PSU'
        sal_ctxt.fill_value = 0x0
        context_ids.append(self.parameter_management.create_parameter_context(name='salinity', parameter_context=sal_ctxt.dump()))

        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.float32))
        temp_ctxt.uom = 'degree_Celsius'
        temp_ctxt.fill_value = 0e0
        context_ids.append(self.parameter_management.create_parameter_context(name='temp', parameter_context=temp_ctxt.dump()))

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.int64))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0
        context_ids.append(self.parameter_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump()))

        return context_ids




