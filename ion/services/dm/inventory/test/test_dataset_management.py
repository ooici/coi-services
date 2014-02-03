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

from coverage_model import QuantityType, ParameterContext, ParameterDictionary, AxisTypeEnum, NumexprFunction, ParameterFunctionType, VariabilityEnum, PythonFunction, ConstantType

from nose.plugins.attrib import attr
from pyon.public import PRED, RT

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
        ds_obj2 = self.dataset_management.read_dataset(dataset_id)
        self.assertEquals(ds_obj.name, ds_obj2.name)
    
    def test_context_crud(self):
        context_ids = self.create_contexts()
        context_id = context_ids.pop()

        context = DatasetManagementService.get_parameter_context(context_id)
        self.assertIsInstance(context, ParameterContext)
        self.assertEquals(context.identifier, context_id)

        self.dataset_management.delete_parameter_context(context_id)

        with self.assertRaises(NotFound):
            self.dataset_management.read_parameter_context(context_id)



    def test_pfunc_crud(self):
        contexts, funcs = self.create_pfuncs()
        context_ids = [context_id for ctxt,context_id in contexts.itervalues()]

        pdict_id = self.dataset_management.create_parameter_dictionary(name='functional_pdict', parameter_context_ids=context_ids, temporal_context='time')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, pdict_id)

        expr, expr_id = funcs['CONDWAT_L1']
        func_class = DatasetManagementService.get_parameter_function(expr_id)
        dpd_ids, _ = self.resource_registry.find_subjects(object=expr_id, predicate=PRED.hasParameterFunction, subject_type=RT.DataProcessDefinition)
        # Verify that there is a data process definition for each one
        self.assertTrue(dpd_ids)
        self.assertIsInstance(func_class, NumexprFunction)

    def test_pdict_crud(self):
        context_ids = self.create_contexts()
        pdict_res_id = self.dataset_management.create_parameter_dictionary(name='pdict1', parameter_context_ids=context_ids, temporal_context='time')

        pdict_contexts = self.dataset_management.read_parameter_contexts(parameter_dictionary_id=pdict_res_id, id_only=True)

        pdict = DatasetManagementService.get_parameter_dictionary(pdict_res_id)
        self.assertIsInstance(pdict, ParameterDictionary)
        self.assertTrue('time_test' in pdict)
        self.assertEquals(pdict.identifier, pdict_res_id)

        self.assertEquals(set(pdict_contexts), set(context_ids))

        self.dataset_management.delete_parameter_dictionary(parameter_dictionary_id=pdict_res_id)
        with self.assertRaises(NotFound):
            self.dataset_management.read_parameter_dictionary(parameter_dictionary_id=pdict_res_id)

    def create_contexts(self):
        context_ids = []
        cond_ctxt = ParameterContext('conductivity_test', param_type=QuantityType(value_encoding=np.float32))
        cond_ctxt.uom = 'unknown'
        cond_ctxt.fill_value = 0e0
        context_ids.append(self.dataset_management.create_parameter_context(name='conductivity_test', parameter_context=cond_ctxt.dump()))

        pres_ctxt = ParameterContext('pressure_test', param_type=QuantityType(value_encoding=np.float32))
        pres_ctxt.uom = 'Pascal'
        pres_ctxt.fill_value = 0x0
        context_ids.append(self.dataset_management.create_parameter_context(name='pressure_test', parameter_context=pres_ctxt.dump()))

        sal_ctxt = ParameterContext('salinity_test', param_type=QuantityType(value_encoding=np.float32))
        sal_ctxt.uom = 'PSU'
        sal_ctxt.fill_value = 0x0
        context_ids.append(self.dataset_management.create_parameter_context(name='salinity_test', parameter_context=sal_ctxt.dump()))

        temp_ctxt = ParameterContext('temp_test', param_type=QuantityType(value_encoding=np.float32))
        temp_ctxt.uom = 'degree_Celsius'
        temp_ctxt.fill_value = 0e0
        context_ids.append(self.dataset_management.create_parameter_context(name='temp_test', parameter_context=temp_ctxt.dump()))

        t_ctxt = ParameterContext('time_test', param_type=QuantityType(value_encoding=np.int64))
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0
        context_ids.append(self.dataset_management.create_parameter_context(name='time_test', parameter_context=t_ctxt.dump()))

        return context_ids

    def create_pfuncs(self):
        contexts = {}
        funcs = {}

        t_ctxt = ParameterContext('TIME', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1900'
        t_ctxt_id = self.dataset_management.create_parameter_context(name='test_TIME', parameter_context=t_ctxt.dump())
        contexts['TIME'] = (t_ctxt, t_ctxt_id)

        lat_ctxt = ParameterContext('LAT', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))), fill_value=-9999)
        lat_ctxt.axis = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt_id = self.dataset_management.create_parameter_context(name='test_LAT', parameter_context=lat_ctxt.dump())
        contexts['LAT'] = lat_ctxt, lat_ctxt_id

        lon_ctxt = ParameterContext('LON', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))), fill_value=-9999)
        lon_ctxt.axis = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt_id = self.dataset_management.create_parameter_context(name='test_LON', parameter_context=lon_ctxt.dump())
        contexts['LON'] = lon_ctxt, lon_ctxt_id

        # Independent Parameters

        # Temperature - values expected to be the decimal results of conversion from hex
        temp_ctxt = ParameterContext('TEMPWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        temp_ctxt.uom = 'deg_C'
        temp_ctxt_id = self.dataset_management.create_parameter_context(name='test_TEMPWAT_L0', parameter_context=temp_ctxt.dump())
        contexts['TEMPWAT_L0'] = temp_ctxt, temp_ctxt_id

        # Conductivity - values expected to be the decimal results of conversion from hex
        cond_ctxt = ParameterContext('CONDWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        cond_ctxt.uom = 'S m-1'
        cond_ctxt_id = self.dataset_management.create_parameter_context(name='test_CONDWAT_L0', parameter_context=cond_ctxt.dump())
        contexts['CONDWAT_L0'] = cond_ctxt, cond_ctxt_id

        # Pressure - values expected to be the decimal results of conversion from hex
        press_ctxt = ParameterContext('PRESWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        press_ctxt.uom = 'dbar'
        press_ctxt_id = self.dataset_management.create_parameter_context(name='test_PRESWAT_L0', parameter_context=press_ctxt.dump())
        contexts['PRESWAT_L0'] = press_ctxt, press_ctxt_id


        # Dependent Parameters

        # TEMPWAT_L1 = (TEMPWAT_L0 / 10000) - 10
        tl1_func = '(T / 10000) - 10'
        expr = NumexprFunction('TEMPWAT_L1', tl1_func, ['T'])
        expr_id = self.dataset_management.create_parameter_function(name='test_TEMPWAT_L1', parameter_function=expr.dump())
        funcs['TEMPWAT_L1'] = expr, expr_id

        tl1_pmap = {'T': 'TEMPWAT_L0'}
        expr.param_map = tl1_pmap
        tempL1_ctxt = ParameterContext('TEMPWAT_L1', param_type=ParameterFunctionType(function=expr), variability=VariabilityEnum.TEMPORAL)
        tempL1_ctxt.uom = 'deg_C'
        tempL1_ctxt_id = self.dataset_management.create_parameter_context(name='test_TEMPWAT_L1', parameter_context=tempL1_ctxt.dump(), parameter_function_id=expr_id)
        contexts['TEMPWAT_L1'] = tempL1_ctxt, tempL1_ctxt_id

        # CONDWAT_L1 = (CONDWAT_L0 / 100000) - 0.5
        cl1_func = '(C / 100000) - 0.5'
        expr = NumexprFunction('CONDWAT_L1', cl1_func, ['C'])
        expr_id = self.dataset_management.create_parameter_function(name='test_CONDWAT_L1', parameter_function=expr.dump())
        funcs['CONDWAT_L1'] = expr, expr_id

        cl1_pmap = {'C': 'CONDWAT_L0'}
        expr.param_map = cl1_pmap
        condL1_ctxt = ParameterContext('CONDWAT_L1', param_type=ParameterFunctionType(function=expr), variability=VariabilityEnum.TEMPORAL)
        condL1_ctxt.uom = 'S m-1'
        condL1_ctxt_id = self.dataset_management.create_parameter_context(name='test_CONDWAT_L1', parameter_context=condL1_ctxt.dump(), parameter_function_id=expr_id)
        contexts['CONDWAT_L1'] = condL1_ctxt, condL1_ctxt_id

        # Equation uses p_range, which is a calibration coefficient - Fixing to 679.34040721
        #   PRESWAT_L1 = (PRESWAT_L0 * p_range / (0.85 * 65536)) - (0.05 * p_range)
        pl1_func = '(P * p_range / (0.85 * 65536)) - (0.05 * p_range)'
        expr = NumexprFunction('PRESWAT_L1', pl1_func, ['P', 'p_range'])
        expr_id = self.dataset_management.create_parameter_function(name='test_PRESWAT_L1', parameter_function=expr.dump())
        funcs['PRESWAT_L1'] = expr, expr_id
        
        pl1_pmap = {'P': 'PRESWAT_L0', 'p_range': 679.34040721}
        expr.param_map = pl1_pmap
        presL1_ctxt = ParameterContext('PRESWAT_L1', param_type=ParameterFunctionType(function=expr), variability=VariabilityEnum.TEMPORAL)
        presL1_ctxt.uom = 'S m-1'
        presL1_ctxt_id = self.dataset_management.create_parameter_context(name='test_CONDWAT_L1', parameter_context=presL1_ctxt.dump(), parameter_function_id=expr_id)
        contexts['PRESWAT_L1'] = presL1_ctxt, presL1_ctxt_id

        # Density & practical salinity calucluated using the Gibbs Seawater library - available via python-gsw project:
        #       https://code.google.com/p/python-gsw/ & http://pypi.python.org/pypi/gsw/3.0.1

        # PRACSAL = gsw.SP_from_C((CONDWAT_L1 * 10), TEMPWAT_L1, PRESWAT_L1)
        owner = 'gsw'
        sal_func = 'SP_from_C'
        sal_arglist = ['C', 't', 'p']
        expr = PythonFunction('PRACSAL', owner, sal_func, sal_arglist)
        expr_id = self.dataset_management.create_parameter_function(name='test_PRACSAL', parameter_function=expr.dump())
        funcs['PRACSAL'] = expr, expr_id
        
        # A magic function that may or may not exist actually forms the line below at runtime.
        sal_pmap = {'C': NumexprFunction('CONDWAT_L1*10', 'C*10', ['C'], param_map={'C': 'CONDWAT_L1'}), 't': 'TEMPWAT_L1', 'p': 'PRESWAT_L1'}
        expr.param_map = sal_pmap
        sal_ctxt = ParameterContext('PRACSAL', param_type=ParameterFunctionType(expr), variability=VariabilityEnum.TEMPORAL)
        sal_ctxt.uom = 'g kg-1'
        sal_ctxt_id = self.dataset_management.create_parameter_context(name='test_PRACSAL', parameter_context=sal_ctxt.dump(), parameter_function_id=expr_id)
        contexts['PRACSAL'] = sal_ctxt, sal_ctxt_id

        # absolute_salinity = gsw.SA_from_SP(PRACSAL, PRESWAT_L1, longitude, latitude)
        # conservative_temperature = gsw.CT_from_t(absolute_salinity, TEMPWAT_L1, PRESWAT_L1)
        # DENSITY = gsw.rho(absolute_salinity, conservative_temperature, PRESWAT_L1)
        owner = 'gsw'
        abs_sal_expr = PythonFunction('abs_sal', owner, 'SA_from_SP', ['PRACSAL', 'PRESWAT_L1', 'LON','LAT'])
        cons_temp_expr = PythonFunction('cons_temp', owner, 'CT_from_t', [abs_sal_expr, 'TEMPWAT_L1', 'PRESWAT_L1'])
        dens_expr = PythonFunction('DENSITY', owner, 'rho', [abs_sal_expr, cons_temp_expr, 'PRESWAT_L1'])
        dens_ctxt = ParameterContext('DENSITY', param_type=ParameterFunctionType(dens_expr), variability=VariabilityEnum.TEMPORAL)
        dens_ctxt.uom = 'kg m-3'
        dens_ctxt_id = self.dataset_management.create_parameter_context(name='test_DENSITY', parameter_context=dens_ctxt.dump())
        contexts['DENSITY'] = dens_ctxt, dens_ctxt_id
        return contexts, funcs

    def test_verify_contexts(self):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name(name='ctd_parsed_param_dict', id_only=True)
        pcontexts = self.dataset_management.read_parameter_contexts(parameter_dictionary_id=pdict_id)
        for pcontext in pcontexts:
            self.assertTrue('fill_value' in pcontext)
            self.assertTrue('reference_urls' in pcontext)
            self.assertTrue('internal_name' in pcontext)
            self.assertTrue('display_name' in pcontext)
            self.assertTrue('standard_name' in pcontext)
            self.assertTrue('ooi_short_name' in pcontext)
            self.assertTrue('description' in pcontext)
            self.assertTrue('precision' in pcontext)


